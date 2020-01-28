package cassandra

import "testing"

func TestCassandra_Name(t *testing.T) {
	cass := New()
	if cass.Name() != "cassandra" {
		t.Error()
	}
	if cass.keyspace != "coredns" {
		t.Error()
	}
}
