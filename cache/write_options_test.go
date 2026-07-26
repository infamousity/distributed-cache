package cache

import (
	"testing"

	"github.com/infamousity/distributed-cache/config"
)

func TestProgrammaticWriteConcernDefaultRemainsOne(t *testing.T) {
	if WriteConcernOne != 0 || WriteConcernMajority != 1 {
		t.Fatalf("legacy write concern values changed: one=%d majority=%d", WriteConcernOne, WriteConcernMajority)
	}
	if got := (Options{}).withDefaults().WriteConcern; got != WriteConcernOne {
		t.Fatalf("programmatic default write concern = %v, want one", got)
	}
}

func TestWriteConcernConfigurationValues(t *testing.T) {
	tests := []struct {
		value string
		want  WriteConcern
	}{
		{value: "one", want: WriteConcernOne},
		{value: "majority", want: WriteConcernMajority},
		{value: "quorum", want: WriteConcernMajority},
		{value: "all", want: WriteConcernAll},
		{value: "", want: WriteConcernMajority},
	}
	for _, test := range tests {
		if got := parseWriteConcern(test.value); got != test.want {
			t.Errorf("parseWriteConcern(%q) = %v, want %v", test.value, got, test.want)
		}
	}
}

func TestValidateConfigRejectsUnknownWriteConcern(t *testing.T) {
	cfg := config.Config{}
	cfg.Common.Cache.WriteConcern = "fast"
	if err := validateConfig(&cfg, Options{WriteConcern: WriteConcernMajority}); err == nil {
		t.Fatal("expected unknown write concern to fail validation")
	}
}
