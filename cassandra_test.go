package cassandra

import (
	"os"
	"testing"
)

var (
	cassandraTest *CassandraDatastore
	zone          = "test.zone."
	hosts         = []string{"127.0.0.1"}
)

func TestMain(m *testing.M) {
	//cluster := gocql.NewCluster("localhost")
	//cluster.Keyspace = "zones"
	//session, err := cluster.CreateSession()

	//if err != nil {
	//	panic(err)
	//}
	//
	//if err := session.Query("truncate rr;").Exec(); err != nil {
	//	panic(err)
	//}
	//
	//if err := session.Query("truncate soa;").Exec(); err != nil {
	//	panic(err)
	//}
	//
	//cassandraTest = NewCassandraDatastore(hosts, "test")

	os.Exit(m.Run())
}

func TestCassandra_ServeDNS(t *testing.T) {

}
