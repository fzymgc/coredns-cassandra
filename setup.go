package cassandra

import (
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/mholt/caddy"
)

func init() {
	caddy.RegisterPlugin("cassandra", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	// This is currently hard-coded.  Need to pull this from a configuration somwhere (Corefile; env?)
	hosts := []string{"127.0.0.1"}
	db := NewCassandraDatastore(hosts, "zones")
	k := NewCassandraPlugin(db)

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		k.Next = next
		return k
	})

	return nil
}
