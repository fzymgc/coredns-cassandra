package cassandra

import (
	"github.com/miekg/dns"
	"net"
	"os"
	"testing"
)

var (
	cassandraTest *Cassandra
	zone          = "test.zone."
)

func TestMain(m *testing.M) {
	cassandraTest = newCassandraPlugin("localhost", "fortify_dns")
	os.Exit(m.Run())
}

func TestCassandra_CreateZone(t *testing.T) {
	err := cassandraTest.CreateZone(zone)
	if err != nil {
		t.Error(err)
	}
}

func TestCassandra_InsertRecord(t *testing.T) {
	testHeader := dns.RR_Header{
		Name:   "record.",
		Rrtype: dns.TypeA,
		Class:  dns.ClassINET,
		Ttl:    300,
	}

	rr := &dns.A{
		Hdr: testHeader,
		A:   net.ParseIP("1.1.1.1"),
	}

	err := cassandraTest.InsertRecord(zone, rr)
	if err != nil {
		t.Error(err)
	}
}

func TestCassandra_Zones(t *testing.T) {
	err := cassandraTest.CreateZone(zone)
	if err != nil {
		t.Error(err)
	}

	zones := cassandraTest.Zones()
	if len(zones) != 1 {
		t.Error("incorrect number of zones")
	}

	if zones[0] != zone {
		t.Error("zone names do not match")
	}
}

func TestCassandra_GetRecords(t *testing.T) {
	answers, extras, err := cassandraTest.GetRecords(zone, "record.", dns.TypeA, dns.ClassINET)
	if err != nil {
		t.Fatal(err)
	}

	if len(answers) != 1 {
		t.Fatalf("incorrect response. expected 1 record, received %v", len(answers))
	}

	if len(extras) != 0 {
		t.Fatal("received unexpected extras response")
	}
}
