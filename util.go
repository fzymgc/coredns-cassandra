package cassandra

import (
	"github.com/miekg/dns"
	"strings"
)

func ParseHostFromQname(qname, zone string) string {
	qTokens := dns.SplitDomainName(qname)
	zTokens := dns.SplitDomainName(zone)

	length := len(qTokens) - len(zTokens)
	return dns.Fqdn(strings.Join(qTokens[0:length], "."))
}
