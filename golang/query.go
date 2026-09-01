package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gocql/gocql"
)

// Flags mirror query.py so both tools can be driven the same way.
var (
	hostsFlag   = flag.String("hosts", "scylla-client", "Comma-separated ScyllaDB node names or IPs")
	username    = flag.String("username", "cassandra", "ScyllaDB username")
	password    = flag.String("password", "cassandra", "ScyllaDB password")
	keyspace    = flag.String("keyspace", "mykeyspace", "Keyspace name")
	table       = flag.String("table", "mytable", "Table name")
	rowCount    = flag.Int("row_count", 100000, "Highest row id written by the loader")
	idOffset    = flag.Int("offset", 0, "ID offset (must match loader)")
	numBuckets  = flag.Int("buckets", 256, "Partition bucket count (must match loader: id % buckets)")
	localOnly   = flag.Bool("local_only", false, "Use local-only mode (single node, no host discovery)")
	consistency = flag.String("cl", "LOCAL_QUORUM", "Consistency level (ONE, TWO, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM)")
	dc          = flag.String("dc", "dc1", "Local datacenter name for ScyllaDB")
	minutes     = flag.Float64("minutes", 60, "How long to run the query loop (minutes)")
	interval    = flag.Float64("interval", 0.01, "Delay between queries (seconds)")
)

func init() {
	// Short forms, as in query.py: -s -u -p -k -t -r -o -l
	flag.StringVar(hostsFlag, "s", *hostsFlag, "Shorthand for -hosts")
	flag.StringVar(username, "u", *username, "Shorthand for -username")
	flag.StringVar(password, "p", *password, "Shorthand for -password")
	flag.StringVar(keyspace, "k", *keyspace, "Shorthand for -keyspace")
	flag.StringVar(table, "t", *table, "Shorthand for -table")
	flag.IntVar(rowCount, "r", *rowCount, "Shorthand for -row_count")
	flag.IntVar(idOffset, "o", *idOffset, "Shorthand for -offset")
	flag.BoolVar(localOnly, "l", *localOnly, "Shorthand for -local_only")
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags)

	if *numBuckets < 1 {
		log.Fatal("-buckets must be >= 1")
	}

	cl, err := gocql.ParseConsistencyWrapper(*consistency)
	if err != nil {
		log.Fatalf("Invalid consistency level %q: %v", *consistency, err)
	}

	hosts := splitHosts(*hostsFlag)
	if len(hosts) == 0 {
		log.Fatal("-hosts must name at least one node")
	}

	log.Printf("Connecting to cluster: %v with user %s", hosts, *username)
	log.Printf("Using keyspace: %s, table: %s", *keyspace, *table)
	log.Printf("Local DC: %s", *dc)
	log.Printf("Using consistency level: %s", *consistency)
	log.Printf("Row count to query: %d, id offset: %d, buckets: %d", *rowCount, *idOffset, *numBuckets)

	cluster := gocql.NewCluster(hosts...)

	// ADD AUTHENTICATION HERE
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *username, // Default Scylla superuser
		Password: *password, // Default password
	}

	cluster.Port = 9042
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Consistency = cl

	// Same branch as query.py: a single local node skips host discovery and
	// token awareness, anything else uses a DC-aware, token-aware policy.
	isLocalOnly := *localOnly || hosts[0] == "127.0.0.1" || hosts[0] == "localhost"
	if isLocalOnly {
		log.Printf("Using round-robin over a whitelist of hosts: %v", hosts)
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
		cluster.HostFilter = gocql.WhiteListHostFilter(hosts...)
		cluster.DisableInitialHostLookup = true
	} else {
		log.Printf("Using TokenAwareHostPolicy with local_dc: %s", *dc)
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(*dc))
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer session.Close()

	log.Printf("✅ Connected to Scylla with auth! (user: %s, password: %s)", *username, strings.Repeat("*", len(*password)))

	var version string
	err = session.Query(`SELECT version FROM system.versions WHERE key = 'local';`).Scan(&version)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	log.Println("Scylla version:", version)

	clients, err := countRows(session, `SELECT * FROM system.clients`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	log.Println("Connected clients:", clients)

	runner := &queryRunner{session: session}
	runner.runForDuration(*minutes, *interval)
}

// queryRunner mirrors TableQueryRunner in query.py: it hits a single partition
// per iteration using a random id, for a fixed duration, then reports totals.
type queryRunner struct {
	session    *gocql.Session
	queryCount int
	errorCount int
}

// query is the statement prepared once and reused; gocql prepares it on first
// use and caches it per connection.
func (r *queryRunner) query() string {
	return fmt.Sprintf("SELECT * FROM %s.%s WHERE bucket = ? AND id = ?", *keyspace, *table)
}

func (r *queryRunner) executeQuery() bool {
	rid := *idOffset + rand.Intn(*rowCount) + 1
	bucket := rid % *numBuckets

	iter := r.session.Query(r.query(), bucket, rid).Iter()

	var rows []map[string]interface{}
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		rows = append(rows, row)
		row = make(map[string]interface{}) // reset map for next row
	}

	if err := iter.Close(); err != nil {
		r.errorCount++
		log.Printf("Query #%d failed: %v", r.queryCount+1, err)
		return false
	}

	r.queryCount++
	log.Printf("Query #%d bucket=%d id=%d -> %d rows", r.queryCount, bucket, rid, len(rows))
	if len(rows) > 0 {
		log.Printf("Sample: %s", formatRow(rows[0]))
	} else {
		log.Println("No rows returned from main table")
	}
	return true
}

func (r *queryRunner) runForDuration(durationMinutes, intervalSeconds float64) {
	log.Printf("Starting query runner for %g minutes...", durationMinutes)
	log.Printf("Prepared query: %s", r.query())

	// Ctrl-C ends the loop early but still prints the final statistics.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	start := time.Now()
	end := start.Add(time.Duration(durationMinutes * float64(time.Minute)))
	delay := time.Duration(intervalSeconds * float64(time.Second))

	for time.Now().Before(end) {
		r.executeQuery()
		if r.queryCount%50 == 0 && r.queryCount != 0 {
			log.Printf("Progress: %d queries, %d errors, elapsed: %s", r.queryCount, r.errorCount, time.Since(start).Round(time.Second))
		}
		select {
		case <-stop:
			log.Println("Script interrupted by user")
			r.printStats(time.Since(start))
			return
		case <-time.After(delay):
		}
	}
	r.printStats(time.Since(start))
}

func (r *queryRunner) printStats(total time.Duration) {
	successRate := 0.0
	if r.queryCount > 0 {
		successRate = float64(r.queryCount-r.errorCount) / float64(r.queryCount) * 100
	}
	log.Println("=== Final Statistics ===")
	log.Printf("Total runtime: %s", total.Round(time.Millisecond))
	log.Printf("Total queries: %d", r.queryCount)
	log.Printf("Successful queries: %d", r.queryCount-r.errorCount)
	log.Printf("Failed queries: %d", r.errorCount)
	log.Printf("Success rate: %.2f%%", successRate)
	log.Printf("Average queries per second: %.2f", float64(r.queryCount)/total.Seconds())
}

func splitHosts(s string) []string {
	var hosts []string
	for _, h := range strings.Split(s, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}

// countRows runs a query for its row count only, without printing each row.
func countRows(session *gocql.Session, stmt string) (int, error) {
	iter := session.Query(stmt).Iter()
	count := 0
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		count++
		row = make(map[string]interface{}) // reset map for next row
	}
	return count, iter.Close()
}

// formatRow renders a row as compact "key=value" pairs, skipping the bulky
// free-text columns that make the raw map dump unreadable.
func formatRow(row map[string]interface{}) string {
	skip := map[string]bool{"message": true}

	keys := make([]string, 0, len(row))
	for k := range row {
		if !skip[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, row[k]))
	}
	return strings.Join(parts, " ")
}
