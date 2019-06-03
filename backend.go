package cassandra

import "github.com/miekg/dns"

type CassandraBackend interface {
	Zones() []string
	CreateZone(name string) error
	GetRecords(zone string, host string, qtype uint16, class uint16) ([]dns.RR, []dns.RR, error)
	InsertRecord(zone string, rr dns.RR) error
}
