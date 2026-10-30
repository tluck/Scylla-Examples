package main

import (
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// getenv returns the value of the environment variable key, or def if unset.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	// Settings come from the same environment variables as the Python and Java
	// samples (see psc_env.bash). Two connection models:
	//   psc    the PSC endpoint DNS name (ScyllaDB Cloud PrivateLink tab) with client routes
	//   direct the node addresses, e.g. over VPC peering
	connectionIDs := os.Getenv("SCYLLA_PSC_CONN_ID")
	mode := getenv("SCYLLA_MODE", map[bool]string{true: "psc", false: "direct"}[connectionIDs != ""])
	caFile := os.Getenv("SCYLLA_CA_FILE")
	tlsName := os.Getenv("SCYLLA_TLS_NAME")
	defaultPort := "9000"
	if mode == "direct" {
		// the driver itself dials the shard-aware port (19042/19142) it learns from the node
		defaultPort = map[bool]string{true: "9142", false: "9042"}[caFile != ""]
	}
	port, err := strconv.Atoi(getenv("SCYLLA_PORT", getenv("SCYLLA_PSC_PORT", defaultPort)))
	if err != nil {
		panic(fmt.Sprintf("Invalid SCYLLA_PORT: %v", err))
	}
	password := os.Getenv("SCYLLA_PASSWORD")
	if password == "" {
		panic("SCYLLA_PASSWORD is not set")
	}
	dc := os.Getenv("SCYLLA_DC")

	var cluster *gocql.ClusterConfig
	switch mode {
	case "psc":
		endpoint := os.Getenv("SCYLLA_PSC_DNS")
		if endpoint == "" || connectionIDs == "" {
			panic("psc mode needs SCYLLA_PSC_DNS and SCYLLA_PSC_CONN_ID")
		}
		cluster = gocql.NewCluster(endpoint)
		// Apply the PrivateLink routing configuration, one endpoint per client route ID
		var endpoints []gocql.ClientRoutesEndpoint
		for _, id := range strings.Split(connectionIDs, ",") {
			endpoints = append(endpoints, gocql.ClientRoutesEndpoint{ConnectionID: strings.TrimSpace(id)})
		}
		cluster.WithOptions(gocql.WithClientRoutes(gocql.WithEndpoints(endpoints...)))
	case "direct":
		hosts := os.Getenv("SCYLLA_CONTACT_POINTS")
		if hosts == "" {
			panic("direct mode needs SCYLLA_CONTACT_POINTS")
		}
		cluster = gocql.NewCluster(strings.Split(hosts, ",")...)
	default:
		panic("SCYLLA_MODE must be psc or direct, not " + mode)
	}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: getenv("SCYLLA_USER", "scylla"),
		Password: password,
	}

	// TLS: in psc mode, with SslOpts set the driver uses tls_port from
	// system.client_routes. Neither the PSC DNS name nor a node address is what the
	// certificates are checked against: the cluster TLS name is in every node
	// certificate, so it is the ServerName (SNI and verification) in both modes.
	// Without a TLS name only the CA chain is checked.
	if caFile != "" {
		tlsConfig := &tls.Config{ServerName: tlsName}
		if tlsName == "" {
			tlsConfig.InsecureSkipVerify = true
		}
		cluster.SslOpts = &gocql.SslOptions{Config: tlsConfig, CaPath: caFile, EnableHostVerification: tlsName != ""}
	}

	// Standard cluster tuning
	cluster.Port = port
	cluster.Timeout = 5 * time.Second
	if dc != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(dc))
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}

	session, err := cluster.CreateSession()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect via PrivateLink: %v", err))
	}
	defer session.Close()

	tlsMode := "plain"
	if caFile != "" {
		tlsMode = "TLS, CA only"
		if tlsName != "" {
			tlsMode = "TLS, verified " + tlsName
		}
	}
	fmt.Printf("Connection established (%s, %s)!\n", map[string]string{"psc": "PSC with ClientRoutes", "direct": "direct to the nodes"}[mode], tlsMode)

	// Query execution
	query := session.Query("SELECT * FROM system.clients")
	if rows, err := query.Iter().SliceMap(); err == nil {
		for _, row := range rows {
			fmt.Printf("%v\n", row)
		}
	} else {
		panic("Query error: " + err.Error())
	}
}
