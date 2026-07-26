package main

import (
	"testing"

	dcache "github.com/infamousity/distributed-cache/cache"
)

func TestParseWriteConcern(t *testing.T) {
	tests := []struct {
		value string
		want  dcache.WriteConcern
	}{
		{value: "one", want: dcache.WriteConcernOne},
		{value: "majority", want: dcache.WriteConcernMajority},
		{value: "quorum", want: dcache.WriteConcernMajority},
		{value: "all", want: dcache.WriteConcernAll},
	}
	for _, test := range tests {
		got, err := parseWriteConcern(test.value)
		if err != nil {
			t.Errorf("parseWriteConcern(%q): %v", test.value, err)
			continue
		}
		if got != test.want {
			t.Errorf("parseWriteConcern(%q) = %v, want %v", test.value, got, test.want)
		}
	}
	if _, err := parseWriteConcern("fast"); err == nil {
		t.Fatal("expected invalid write concern to fail")
	}
}
