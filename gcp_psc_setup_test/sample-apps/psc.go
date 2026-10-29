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
	// samples (see psc_env.bash). The DNS name is the one provided in your
	// ScyllaDB Cloud PrivateLink tab.
	endpoint := getenv("SCYLLA_PSC_DNS", "endpoint.cluster-1.scylladb.com")
	connectionIDs := getenv("SCYLLA_PSC_CONN_ID", "1")
	port, err := strconv.Atoi(getenv("SCYLLA_PSC_PORT", "9000"))
	if err != nil {
		panic(fmt.Sprintf("Invalid SCYLLA_PSC_PORT: %v", err))
	}
	password := os.Getenv("SCYLLA_PASSWORD")
	if password == "" {
		panic("SCYLLA_PASSWORD is not set")
	}
	dc := os.Getenv("SCYLLA_DC")
	caFile := os.Getenv("SCYLLA_CA_FILE")
	tlsName := os.Getenv("SCYLLA_TLS_NAME")

	cluster := gocql.NewCluster(endpoint)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: getenv("SCYLLA_USER", "scylla"),
		Password: password,
	}

	// Apply the PrivateLink routing configuration, one endpoint per client route ID
	var endpoints []gocql.ClientRoutesEndpoint
	for _, id := range strings.Split(connectionIDs, ",") {
		endpoints = append(endpoints, gocql.ClientRoutesEndpoint{ConnectionID: strings.TrimSpace(id)})
	}
	cluster.WithOptions(gocql.WithClientRoutes(gocql.WithEndpoints(endpoints...)))

	// TLS (a TLS PSC connection): with SslOpts set the driver uses tls_port from
	// system.client_routes. The PSC DNS name is not in the node certificates, the
	// cluster TLS name is, so it is the ServerName (SNI and verification).
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

	mode := "plain"
	if caFile != "" {
		mode = "TLS, CA only"
		if tlsName != "" {
			mode = "TLS, verified " + tlsName
		}
	}
	fmt.Printf("Connection established using ClientRoutes (%s)!\n", mode)

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
