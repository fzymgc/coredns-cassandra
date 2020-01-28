package cassandra

import (
	"context"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"time"
)

// main entrypoint from coredns
func (cass *Cassandra) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	qname := state.Name()
	qtype := state.QType()
	clog.Infof("ServeDNS: qname=%s, qtype=%s",qname, state.Type())

	if time.Since(cass.LastZoneUpdate) > zoneUpdateTime {
		cass.LoadZones()
	}

	zone := plugin.Zones(cass.Zones).Matches(qname)
	clog.Debugf("attempting to serve zone: %s",zone)
	if zone == "" {
		return plugin.NextOrFailure(qname, cass.Next, ctx, w, r)
	}

	answers := make([]dns.RR, 0, 10)
	extras := make([]dns.RR, 0, 10)
	var err error

	switch qtype {
	case dns.TypeAXFR, dns.TypeIXFR:
		return errorResponse(state, zone, dns.RcodeNotImplemented, err)

	case dns.TypeA:
		answers, extras, err = cass.A(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeAAAA:
		answers, extras, err = cass.AAAA(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeCNAME:
		answers, extras, err = cass.CNAME(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeTXT:
		answers, extras, err = cass.TXT(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeNS:
		answers, extras, err = cass.NS(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeMX:
		answers, extras, err = cass.MX(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeSRV:
		answers, extras, err = cass.SRV(zone,qname)
		if err != nil {
			panic(err)
		}
	case dns.TypeSOA:
		answers, extras, err = cass.SOA(zone,qname)
		if err != nil {
			panic(err)
		}
	default:
		return errorResponse(state, zone, dns.RcodeNotImplemented, err)
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


func errorResponse(state request.Request, zone string, rcode int, err error) (int, error) {
	m := new(dns.Msg)
	m.SetRcode(state.Req, rcode)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

	state.SizeAndDo(m)
	_ = state.W.WriteMsg(m)
	// Return success as the rcode to signal we have written to the client.
	return dns.RcodeSuccess, err
}
