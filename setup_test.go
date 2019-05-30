package cassandra

import "testing"

func TestCassandra_Name(t *testing.T) {
	k := &Cassandra{}
	if k.Name() != "cassandra" {
		t.Error()
	}
}
