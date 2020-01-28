package cassandra

import (
	"encoding/json"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"github.com/gocql/gocql"
	"github.com/miekg/dns"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"time"
)

type Cassandra struct {
	Next           plugin.Handler
	LastZoneUpdate time.Time
	Zones          []string
	Ttl            uint32

	cluster *gocql.ClusterConfig
	session *gocql.Session

	allowTransfers bool
	contactPoints  []string
	username       string
	password       string
	keyspace       string
	consistency    gocql.Consistency
}

func New() *Cassandra {
	return &Cassandra{
		contactPoints: []string{"127.0.0.1"},
		keyspace:      "coredns",
		consistency:   gocql.Quorum,
	}
}

func (cass *Cassandra) Connect() {
	cluster := gocql.NewCluster(cass.contactPoints...)
	cluster.Keyspace = cass.keyspace
	cluster.Consistency = cass.consistency
	cluster.ProtoVersion = 4
	if cass.username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cass.username,
			Password: cass.password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	clog.Infof("Connected to cassandra. contactPoints=%s, keyspace=%s", cass.contactPoints, cass.keyspace)
	cass.session = session
	cass.allowTransfers = true
}

func (cass *Cassandra) LoadZones() {
	iter := cass.session.Query("SELECT DISTINCT zone FROM rr").Iter()
	zones := make([]string, 0, iter.NumRows())
	var zone string
	for iter.Scan(&zone) {
		zones = append(zones, zone)
	}

	cass.LastZoneUpdate = time.Now()
	cass.Zones = zones
	clog.Debugf("loaded zones from db: %s",zones)
}

func (cass *Cassandra) Name() string { return coreDNSPackageName }

func (cass *Cassandra) minTtl(ttl uint32) uint32 {
	if cass.Ttl == 0 && ttl == 0 {
		return defaultTtl
	}
	if cass.Ttl == 0 {
		return ttl
	}
	if ttl == 0 {
		return cass.Ttl
	}
	if cass.Ttl < ttl {
		return cass.Ttl
	}
	return ttl
}

func (cass *Cassandra) A(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "A")

	for _, result := range records {
		rec := &A_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if rec.Ip == nil {
			clog.Debugf("No IP found for qname ( %s ) skipping", qname)
			continue
		}
		a := new(dns.A)
		a.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeA,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		a.A = rec.Ip

		answers = append(answers, a)
	}

	return answers, extras, nil
}

func (cass *Cassandra) AAAA(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "AAAA")

	for _, result := range records {
		rec := &AAAA_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if rec.Ip == nil {
			clog.Debugf("No IP found for qname ( %s ) skipping", qname)
			continue
		}
		aaaa := new(dns.AAAA)
		aaaa.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeAAAA,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		aaaa.AAAA = rec.Ip

		answers = append(answers, aaaa)
	}

	return answers, extras, nil
}

func (cass *Cassandra) CNAME(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "CNAME")

	for _, result := range records {
		rec := &CNAME_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if len(rec.Host) == 0 {
			clog.Debugf("No host found for qname ( %s ) skipping", qname)
			continue
		}
		cname := new(dns.CNAME)
		cname.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeCNAME,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		cname.Target = rec.Host

		answers = append(answers, cname)
	}

	return answers, extras, nil
}

func (cass *Cassandra) TXT(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "TXT")

	for _, result := range records {
		rec := &TXT_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if len(rec.Text) == 0 {
			clog.Debugf("No text found for qname ( %s ) skipping", qname)
			continue
		}
		txt := new(dns.TXT)
		txt.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeTXT,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		txt.Txt = split255(rec.Text)

		answers = append(answers, txt)
	}

	return answers, extras, nil
}

func (cass *Cassandra) NS(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "NS")

	for _, result := range records {
		rec := &NS_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if len(rec.Host) == 0 {
			clog.Debugf("No Host found for qname ( %s ) skipping", qname)
			continue
		}
		ns := new(dns.NS)
		ns.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeNS,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		ns.Ns = rec.Host

		answers = append(answers, ns)
	}

	return answers, extras, nil
}

func (cass *Cassandra) MX(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "MX")

	for _, result := range records {
		rec := &MX_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if len(rec.Host) == 0 {
			clog.Debugf("No Host found for qname ( %s ) skipping", qname)
			continue
		}
		mx := new(dns.MX)
		mx.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeMX,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		mx.Mx = rec.Host
		mx.Preference = rec.Preference
		extras = append(extras, cass.hosts(rec.Host, zone)...)

		answers = append(answers, mx)
	}

	return answers, extras, nil
}

func (cass *Cassandra) SRV(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "SRV")

	for _, result := range records {
		rec := &SRV_Record{}
		err := json.Unmarshal([]byte(result), rec)
		if err != nil {
			clog.Errorf("Unable to parse record: %s", result)
			continue
		}
		if len(rec.Target) == 0 {
			clog.Debugf("No Host found for qname ( %s ) skipping", qname)
			continue
		}
		srv := new(dns.SRV)
		srv.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeSRV,
			Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
		srv.Target = rec.Target
		srv.Weight = rec.Weight
		srv.Port = rec.Port
		srv.Priority = rec.Priority

		answers = append(answers, srv)
		extras = append(extras, cass.hosts(srv.Target, zone)...)
	}

	return answers, extras, nil
}

func (cass *Cassandra) SOA(zone string, qname string) (answers, extras []dns.RR, err error) {
	records, err := cass.fetchRecords(zone, qname, "SOA")

	if len(records) <= 0 {
		soa := new(dns.SOA)
		soa.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeSOA,
			Class: dns.ClassINET, Ttl: cass.Ttl}
		soa.Ns = "ns1." + qname
		soa.Mbox = "hostmaster." + qname
		soa.Refresh = 86400
		soa.Retry = 7200
		soa.Expire = 3600
		soa.Minttl = cass.Ttl
		soa.Serial = cass.serial()
		answers = append(answers, soa)
		return answers, extras, nil
	}

	// should only really ever be one
	result := records[0]
	rec := &SOA_Record{}
	err = json.Unmarshal([]byte(result), rec)
	if err != nil {
		clog.Errorf("Unable to parse record: %s", result)
		return nil, nil, err
	}
	soa := new(dns.SOA)
	soa.Hdr = dns.RR_Header{Name: dns.Fqdn(qname), Rrtype: dns.TypeSOA,
		Class: dns.ClassINET, Ttl: cass.minTtl(rec.Ttl)}
	soa.Ns = rec.Ns
	soa.Mbox = rec.MBox
	soa.Refresh = rec.Refresh
	soa.Retry = rec.Retry
	soa.Expire = rec.Expire
	soa.Minttl = rec.MinTtl
	soa.Serial = cass.serial()

	answers = append(answers, soa)

	return answers, extras, nil

}

func (cass *Cassandra) serial() uint32 {
	return uint32(time.Now().Unix())
}

func (cass *Cassandra) hosts(name string, zone string) (answers []dns.RR) {
	a, _, _ := cass.A(zone, name)
	answers = append(answers, a...)
	aaaa, _, _ := cass.AAAA(zone, name)
	answers = append(answers, aaaa...)
	cname, _, _ := cass.CNAME(zone, name)
	answers = append(answers, cname...)
	return answers
}

func (cass *Cassandra) fetchRecords(zone string, qname string, qtype string) (records []string, err error) {
	host := ParseHostFromQname(qname, zone)

	stmt, names := qb.Select("rr").Where(
		qb.Eq("zone"),
		qb.Eq("name"),
		qb.Eq("rrtype")).Columns("rdata").ToCql()

	qMap := qb.M{
		"zone":   zone,
		"name":   host,
		"rrtype": qtype,
	}
	q := gocqlx.Query(cass.session.Query(stmt), names).BindMap(qMap)
	err = q.SelectRelease(&records)
	if err != nil {
		return nil, err
	}

	return records, nil
}

const (
	zoneUpdateTime = 5 * time.Minute
	defaultTtl     = 300
	transferLength = 1000
)
