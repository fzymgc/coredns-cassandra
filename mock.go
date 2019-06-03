package cassandra

import (
	"github.com/miekg/dns"
)

//CassandraMock is a mock Cassandra datastore for testing purposes.  It has limited support for SOA, A, and CNAME types
type CassandraMock struct {
	soa   map[string]dns.SOA
	a     map[string]map[string]dns.RR
	cname map[string]map[string]dns.RR
}

//NewCassandraMock is a generator for creating new CassandraMock instances
func NewCassandraMock(soa map[string]dns.SOA, a map[string]map[string]dns.RR, cname map[string]map[string]dns.RR) *CassandraMock {
	return &CassandraMock{soa: soa, a: a, cname: cname}
}

func (cm *CassandraMock) Zones() []string {
	zones := make([]string, 0, len(cm.soa))
	for name := range cm.soa {
		zones = append(zones, name)
	}

	return zones
}

func (cm *CassandraMock) CreateZone(name string) error {
	hdr := dns.RR_Header{
		Name: name,
	}
	soa := dns.SOA{
		Hdr:     hdr,
		Ns:      "localhost.",
		Mbox:    "admin.localhost.",
		Serial:  1,
		Refresh: 86400,
		Retry:   7200,
		Expire:  3600,
		Minttl:  300,
	}

	cm.soa[name] = soa
	return nil
}

func (cm *CassandraMock) InsertRecord(zone string, rr dns.RR) error {

	switch rr.Header().Rrtype {
	case dns.TypeA:
		if cm.a[zone] == nil {
			cm.a[zone] = map[string]dns.RR{}
		}
		cm.a[zone][rr.Header().Name] = rr

	case dns.TypeCNAME:
		if cm.cname[zone] == nil {
			cm.cname[zone] = map[string]dns.RR{}
		}
		cm.cname[zone][rr.Header().Name] = rr
	}
	return nil
}

func (cm *CassandraMock) GetRecords(zone string, host string, qtype uint16, class uint16) ([]dns.RR, []dns.RR, error) {
	var answers []dns.RR
	var extras []dns.RR

	switch qtype {
	case dns.TypeA:
		answers = append(answers, cm.a[zone][host])
	case dns.TypeCNAME:
		answers = append(answers, cm.cname[zone][host])
	}

	return answers, extras, nil
}
