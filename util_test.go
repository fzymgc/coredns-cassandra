package cassandra

import "testing"
var (
	zone          = "test.zone."
)

func TestParseHostFromQname(t *testing.T) {
	records := []string{"test.zone.", "record.test.zone.", "long.record.test.zone."}
	expected := []string{"", "record", "long.record"}

	for i := range records {
		parsed := ParseHostFromQname(records[i], zone)
		if expected[i] != parsed {
			t.Errorf("host parse error.  expected %s, but %s was returned", expected[i], parsed)
		}
	}
}
