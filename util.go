package cassandra

import (
	"github.com/miekg/dns"
	"strings"
)

func ParseHostFromQname(qname, zone string) string {
	qTokens := dns.SplitDomainName(qname)
	zTokens := dns.SplitDomainName(zone)

	length := len(qTokens) - len(zTokens)
	return strings.Join(qTokens[0:length], ".")
}

func split255(s string) []string {
	if len(s) < 255 {
		return []string{s}
	}
	sx := []string{}
	p, i := 0, 255
	for {
		if i <= len(s) {
			sx = append(sx, s[p:i])
		} else {
			sx = append(sx, s[p:])
			break

		}
		p, i = p+255, i+255
	}

	return sx
}
