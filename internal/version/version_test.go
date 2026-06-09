package version

import (
	"testing"
	"time"
)

func TestNextAdvancesPastMaxUint32Logical(t *testing.T) {
	const maxUint32 = ^uint32(0)

	observed := Version{Physical: time.Now().Add(time.Hour).UnixMilli(), Logical: uint64(maxUint32), NodeID: "remote"}
	next := observed.Next(time.Now(), "local")

	if next.Compare(observed) <= 0 {
		t.Fatalf("next version %s did not advance past observed version %s", next, observed)
	}
	if next.Physical != observed.Physical {
		t.Fatalf("physical = %d, want %d", next.Physical, observed.Physical)
	}
	if next.Logical != uint64(maxUint32)+1 {
		t.Fatalf("logical = %d, want %d", next.Logical, uint64(maxUint32)+1)
	}
	if next.NodeID != "local" {
		t.Fatalf("nodeID = %q, want local", next.NodeID)
	}
}

func TestNextCarriesAtMaxUint64Logical(t *testing.T) {
	observed := Version{Physical: time.Now().Add(time.Hour).UnixMilli(), Logical: ^uint64(0), NodeID: "remote"}
	next := observed.Next(time.Now(), "local")

	if next.Compare(observed) <= 0 {
		t.Fatalf("next version %s did not advance past observed version %s", next, observed)
	}
	if next.Physical != observed.Physical+1 {
		t.Fatalf("physical = %d, want %d", next.Physical, observed.Physical+1)
	}
	if next.Logical != 0 {
		t.Fatalf("logical = %d, want 0", next.Logical)
	}
	if next.NodeID != "local" {
		t.Fatalf("nodeID = %q, want local", next.NodeID)
	}
}
