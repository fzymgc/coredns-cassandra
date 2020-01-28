package cassandra

import (
	"github.com/caddyserver/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"github.com/gocql/gocql"
)


const (
	coreDNSPackageName string = `cassandra`
)

var log = clog.NewWithPlugin(coreDNSPackageName)



func init() {
	caddy.RegisterPlugin(coreDNSPackageName, caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {

	cass, err := parse(c)
	if err != nil {
		return plugin.Error(coreDNSPackageName,err)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		cass.Next = next
		return cass
	})

	return nil
}

func parse(c *caddy.Controller) (cassandra *Cassandra, err error) {
	cass := New()
	for c.Next() {
		if c.NextBlock() {
			for {
				switch c.Val() {
				case "contact_points":
					if !c.NextArg() {
						return nil, c.ArgErr()
					}
					cass.contactPoints = c.RemainingArgs()
				case "keyspace":
					if !c.NextArg() {
						return nil, c.ArgErr()
					}
					cass.keyspace = c.Val()
				case "consistency":
					if !c.NextArg() {
						return nil, c.ArgErr()
					}
					cass.consistency = gocql.ParseConsistency(c.Val())
				case "username":
					if !c.NextArg() {
						return nil, c.ArgErr()
					}
					cass.username = c.Val()
				case "password":
					if !c.NextArg() {
						return nil, c.ArgErr()
					}
					cass.password = c.Val()
				default:
					if c.Val() != "}" {
						return nil, c.Errf("unknown property '%s'", c.Val())
					}
				}
				if !c.Next() {
					break
				}
			}
		}
	}
	cass.Connect()
	cass.LoadZones()
	return cass, nil
}
