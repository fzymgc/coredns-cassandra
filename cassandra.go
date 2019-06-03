package cassandra

import (
	"context"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

type Cassandra struct {
	Next plugin.Handler
	db   CassandraBackend
}

func NewCassandraPlugin(db CassandraBackend) *Cassandra {
	return &Cassandra{
		db: db,
	}
}

func (c *Cassandra) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}
	db := c.db

	qname := state.Name()
	qtype := state.QType()
	qclass := state.QClass()

	zone := plugin.Zones(db.Zones()).Matches(qname)

	if zone == "" {
		return plugin.NextOrFailure(qname, c.Next, ctx, w, r)
	}

	host := ParseHostFromQname(qname, zone)
	var answers []dns.RR
	var extras []dns.RR
	var err error

	switch qtype {
	case dns.TypeAXFR, dns.TypeIXFR:
		return errorResponse(state, zone, dns.RcodeNotImplemented, err)

	default:
		//answers, extras, err := c.GetRecords(zone, host, qtype)
		answers, extras, err = db.GetRecords(zone, host, qtype, qclass)
		if err != nil {
			// Handle some error here
		}
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

	m.Answer = append(m.Answer, answers...)
	m.Extra = append(m.Extra, extras...)

	state.SizeAndDo(m)
	m = state.Scrub(m)
	_ = w.WriteMsg(m)
	return dns.RcodeSuccess, nil
}

func (c *Cassandra) Name() string { return "cassandra" }
