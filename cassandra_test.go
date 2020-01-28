// +build integration

package cassandra

import (
	"os"
	"testing"
	"time"
)

var (
	cassandraTest *Cassandra
	zone          = "test.zone."
	hosts         = []string{"10.1.0.251","10.1.0.252","10.1.0.253"}
	username = "coredns"
	password = "a8ErHwGfpQ4hCQwyQTcuqyC4s"
	keyspace = "coredns"
)

func TestMain(m *testing.M) {
	// disable test

	startTime := time.Now()
	cassandraTest = New()
	cassandraTest.contactPoints = hosts
	cassandraTest.username = username
	cassandraTest.password = password
	cassandraTest.keyspace = keyspace
	cassandraTest.Connect()
	cassandraTest.LoadZones()

	if cassandraTest.LastZoneUpdate.Before(startTime) {
		panic("Invalid LastZoneUpdate")
	}

	for _, z := range cassandraTest.Zones {
		println(z)
	}

	r, _, _ := cassandraTest.SOA("ftest.zone.","ftest.zone.")
	for _, rr := range r {
		println(rr.String())
	}

	r, _, _ = cassandraTest.A("test.zone.","foo.test.zone.")
	for _, rr := range r {
		println(rr.String())
	}


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
	t.Skip("skipping integration test")
}
