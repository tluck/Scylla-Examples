module scylla-query

go 1.25.7

require github.com/gocql/gocql v1.7.0

// Use the ScyllaDB shard-aware driver. The fork keeps the module path
// github.com/gocql/gocql, so it is wired in with a replace directive
// rather than imported under its own name.
replace github.com/gocql/gocql => github.com/scylladb/gocql v1.19.0

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	golang.org/x/sync v0.22.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)
