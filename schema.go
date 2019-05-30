package cassandra

var (
	// Table Schema
	soaSchema = "CREATE TABLE IF NOT EXISTS soa ( zone text PRIMARY key, ns text, mbox text, serial int, refresh int, retry int, expire int,  minttl int )"
	rrSchema  = "CREATE TABLE IF NOT EXISTS rr (id uuid, zone text, Name text, Rrtype int, Class int, rdata text, PRIMARY KEY ((zone), name, rrtype, class, id) ) WITH CLUSTERING ORDER BY (name ASC, rrtype ASC, class ASC, id ASC)"
)
