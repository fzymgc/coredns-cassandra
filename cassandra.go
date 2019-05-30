package cassandra

import (
	"github.com/coredns/coredns/request"
	"github.com/scylladb/gocqlx/qb"
	"log"

	"github.com/coredns/coredns/plugin"
	"github.com/gocql/gocql"
	"github.com/miekg/dns"
	"github.com/scylladb/gocqlx"
)

type Cassandra struct {
	Next    plugin.Handler
	cluster *gocql.ClusterConfig

	allowTransfers bool
}

func (c *Cassandra) GetRecords(zone string, host string, qtype uint16, class uint16) ([]dns.RR, []dns.RR, error) {
	session, _ := c.cluster.CreateSession()
	defer session.Close()

	var answers []dns.RR
	var extras []dns.RR

	var records []string
	stmt, names := qb.Select("rr").Where(qb.Eq("zone"), qb.Eq("name"), qb.Eq("rrtype"), qb.Eq("class")).Columns("rdata").ToCql()

	qMap := qb.M{
		"zone":   zone,
		"name":   host,
		"rrtype": qtype,
		"class":  class,
	}
	q := gocqlx.Query(session.Query(stmt), names).BindMap(qMap)
	log.Println(q.String())
	if err := q.SelectRelease(&records); err != nil {
		return nil, nil, err
	}

	for _, result := range records {
		rr, err := dns.NewRR(result)
		if err != nil {
			log.Println(err)
			break
		}
		answers = append(answers, rr)
	}

	return answers, extras, nil
}

// Zones returns a list of all zones in Cassandra.  If no zones exist, returns an empty slice.
func (c *Cassandra) Zones() []string {
	session, _ := c.cluster.CreateSession()
	defer session.Close()

	iter := session.Query("SELECT zone FROM soa").Iter()
	zones := make([]string, 0, iter.NumRows())
	var zone string
	for iter.Scan(&zone) {
		zones = append(zones, zone)
	}
	return zones
}

// CreateZone will add a new zone SOA to the database.
func (c *Cassandra) CreateZone(name string) error {
	session, _ := c.cluster.CreateSession()
	defer session.Close()

	soa := qb.M{
		"zone":    name,
		"ns":      "localhost.",
		"mbox":    "localhost.",
		"serial":  1,
		"refresh": 86400,
		"retry":   7200,
		"expire":  3600,
		"minttl":  300,
	}

	stmt, names := qb.Insert("soa").Columns("zone", "ns", "mbox", "serial", "refresh", "retry", "expire", "minttl").ToCql()
	err := gocqlx.Query(session.Query(stmt), names).BindMap(soa).ExecRelease()
	return err
}

func (c *Cassandra) InsertRecord(zone string, rr dns.RR) error {
	session, _ := c.cluster.CreateSession()
	defer session.Close()

	uuid, err := gocql.RandomUUID()
	if err != nil {
		return err
	}

	header := rr.Header()
	rdata := rr.String()

	rrMap := qb.M{
		"id":     uuid,
		"zone":   zone,
		"name":   header.Name,
		"rrtype": header.Rrtype,
		"class":  header.Class,
		"rdata":  rdata,
	}

	//stmt, names := qb.Insert("rr").Columns("id", "zone", "name", "rrtype", "class", "ttl").ToCql()
	stmt, names := qb.Insert("rr").Columns("id", "zone", "name", "rrtype", "class", "rdata").ToCql()
	err = gocqlx.Query(session.Query(stmt), names).BindMap(rrMap).ExecRelease()

	return err
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
