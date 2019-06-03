package cassandra

import (
	"context"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
	"testing"
)

var (
	dnsTestCases = []test.Case{
		{
			Qname: "record.test.zone.",
			Qtype: dns.TypeA,
			Answer: []dns.RR{test.A("record.	IN	A	127.0.0.1")},
		},
	}

	mockStore *CassandraMock
)

func TestNewCassandraMock(t *testing.T) {
	// Create new mock datastore
	mockStore = NewCassandraMock(map[string]dns.SOA{}, map[string]map[string]dns.RR{}, map[string]map[string]dns.RR{})

	// Ensure everything was created correctly
	if len(mockStore.soa) != 0 {
		t.Error("Empty mock store should not have any SOA records")
	}

	if len(mockStore.a) != 0 {
		t.Error("Empty mock store should not have any A records")
	}

	if len(mockStore.cname) != 0 {
		t.Error("Empty mock store should not have any CNAME records")
	}
}

func TestCassandraMock_CreateZone(t *testing.T) {
	_ = mockStore.CreateZone("test.zone.")

	if len(mockStore.soa) != 1 {
		t.Errorf("Expecting there to be 1 SOA record in mock store, but found %v", len(mockStore.soa))
	}
}

func TestCassandraMock_Zones(t *testing.T) {
	_ = mockStore.CreateZone("test.zone.")

	zones := mockStore.Zones()
	if len(zones) != 1 && zones[0] != "test.zone" {
		t.Errorf("Expected 1 zone with name 'test.zone.'")
	}
}

func TestCassandraMock_InsertRecord(t *testing.T) {
	aRecord, _ := dns.NewRR("record.	IN	A	127.0.0.1")
	cnameRecord, _ := dns.NewRR("cname.	IN	CNAME	record.test.zone.")

	_ = mockStore.InsertRecord("test.zone.", aRecord)
	if len(mockStore.a["test.zone."]) != 1 {
		t.Error("expected 1 A record in mock store")
	}

	_ = mockStore.InsertRecord("test.zone.", cnameRecord)
	if len(mockStore.cname["test.zone."]) != 1 {
		t.Error("expected 1 CNAME record in mock store")
	}
}

func TestCassandraMock_GetRecords(t *testing.T) {
	answers, extras, _ := mockStore.GetRecords("test.zone.", "record.", dns.TypeA, dns.ClassINET)
	if len(answers) != 1 && len(extras) != 0 {
		t.Error("expected 1 answer and no extras")
	}

	answers, extras, _ = mockStore.GetRecords("test.zone.", "cname.", dns.TypeCNAME, dns.ClassINET)
	if len(answers) != 1 && len(extras) != 0 {
		t.Error("expected 1 answer and no extras")
	}
}

func TestCassandraMock_ServeDNS(t *testing.T) {
	mockStore = NewCassandraMock(map[string]dns.SOA{}, map[string]map[string]dns.RR{}, map[string]map[string]dns.RR{})
	aRecord, _ := dns.NewRR("record.	IN	A	127.0.0.1")
	cnameRecord, _ := dns.NewRR("cname.	IN	CNAME	record.test.zone.")

	// TODO: Make data population better
	_ = mockStore.CreateZone("test.zone.")
	_ = mockStore.InsertRecord("test.zone.", aRecord)
	_ = mockStore.InsertRecord("test.zone.", cnameRecord)

	c := NewCassandraPlugin(mockStore)

	ctx := context.TODO()

	for _, tc := range dnsTestCases {
		m := tc.Msg()

		rec := dnstest.NewRecorder(&test.ResponseWriter{})
		_, err := c.ServeDNS(ctx, rec, m)
		if err != nil {
			t.Fatal(err)
		}

		resp := rec.Msg
		if err := test.SortAndCheck(resp, tc); err != nil {
			t.Error(err)
		}
	}
}
