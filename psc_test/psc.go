package main

import (
	"fmt"
	"time"
	"github.com/gocql/gocql"
)

func main() {
	// The DNS name provided in your ScyllaDB Cloud PrivateLink tab
	endpoint := "endpoint.cluster-1.scylladb.com" // Example endpoint, replace with your actual endpoint
	connectionID := "1" // your-connection-id-here

	cluster := gocql.NewCluster(endpoint)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "scylla",    // Default Scylla superuser
		Password: "lbjH51uGVMI9cLZ",    // Default password
	}
	// cluster.Authenticator = gocql.PasswordAuthenticator{
	// 	Username: os.Getenv("SCYLLA_USERNAME"),
	// 	Password: os.Getenv("SCYLLA_PASSWORD"),
	// }	

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
	cluster.Port = 9001
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

