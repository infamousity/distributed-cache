package control

import (
	"testing"

	"github.com/infamousity/distributed-cache/internal/controlpb"
)

func TestWriteConcernAllRoundTrips(t *testing.T) {
	if got := toProtoWriteConcern(WriteConcernAll); got != controlpb.WriteConcern_WRITE_CONCERN_ALL {
		t.Fatalf("proto write concern = %v, want all", got)
	}
	if got := fromProtoWriteConcern(controlpb.WriteConcern_WRITE_CONCERN_ALL); got != WriteConcernAll {
		t.Fatalf("internal write concern = %v, want all", got)
	}
}
