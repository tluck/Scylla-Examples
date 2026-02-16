package main

import (
	"fmt"
	"time"
	"github.com/gocql/gocql"
)

func main() {
	//cluster := gocql.NewCluster("10.138.0.150")
	cluster := gocql.NewCluster("127.0.0.1")
	
	// ADD AUTHENTICATION HERE
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",    // Default Scylla superuser
		Password: "cassandra",    // Default password
	}
	
	cluster.Port = 9042
	cluster.Timeout = 10 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	session, err := cluster.CreateSession()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect: %v", err))
	}
	defer session.Close()

	fmt.Println("✅ Connected to Scylla via PSC with auth!")

	var version string
	err = session.Query(`SELECT version FROM system.versions WHERE key = 'local';`).Scan(&version)
	if err != nil {
		panic(fmt.Sprintf("Query failed: %v", err))
	}
	fmt.Println("Scylla version:", version)

        iter := session.Query(`SELECT * FROM system.clients`).Iter()

        row := make(map[string]interface{})
        for iter.MapScan(row) {
            fmt.Println(row)          // or pretty-print
            row = make(map[string]interface{}) // reset map for next row
        }

        if err := iter.Close(); err != nil {
        panic(fmt.Sprintf("Query failed: %v", err))
}

}

