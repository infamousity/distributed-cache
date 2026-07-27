package control

import (
	"testing"

	"github.com/infamousity/distributed-cache/internal/controlpb"
)

func TestWriteConcernAllRoundTrips(t *testing.T) {
	if got := toProtoWriteConcern(WriteConcernAll); got != controlpb.WriteConcern_WRITE_CONCERN_ALL {
		t.Fatalf("proto write concern = %v, want all", got)
	}
	got, err := fromProtoWriteConcern(controlpb.WriteConcern_WRITE_CONCERN_ALL)
	if err != nil {
		t.Fatalf("parse all write concern: %v", err)
	}
	if got != WriteConcernAll {
		t.Fatalf("internal write concern = %v, want all", got)
	}
}

func TestUnknownWriteConcernIsRejected(t *testing.T) {
	if _, err := fromProtoWriteConcern(controlpb.WriteConcern(99)); err == nil {
		t.Fatal("expected unknown wire write concern to be rejected")
	}
}
