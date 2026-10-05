package main

import (
	"fmt"
	"os"
	"strconv"
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
	// Settings come from the same environment variables as sample_python_psc.py
	// (see run_psc_go.bash). The DNS name is the one provided in your
	// ScyllaDB Cloud PrivateLink tab.
	endpoint := getenv("SCYLLA_PSC_DNS", "endpoint.cluster-1.scylladb.com")
	connectionID := getenv("SCYLLA_PSC_CONN_ID", "1")
	port, err := strconv.Atoi(getenv("SCYLLA_PSC_PORT", "9000"))
	if err != nil {
		panic(fmt.Sprintf("Invalid SCYLLA_PSC_PORT: %v", err))
	}
	password := os.Getenv("SCYLLA_PASSWORD")
	if password == "" {
		panic("SCYLLA_PASSWORD is not set")
	}

	cluster := gocql.NewCluster(endpoint)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: getenv("SCYLLA_USER", "scylla"),
		Password: password,
	}

	// Apply the PrivateLink routing configuration
	cluster.WithOptions(
		gocql.WithClientRoutes(
			gocql.WithEndpoints(
				gocql.ClientRoutesEndpoint{
					ConnectionID: connectionID,
				},
			),
		),
	)

	// Standard cluster tuning
	cluster.Port = port
	cluster.Timeout = 5 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	session, err := cluster.CreateSession()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect via PrivateLink: %v", err))
	}
	defer session.Close()

	fmt.Println("Connection established using ClientRoutes!")

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
