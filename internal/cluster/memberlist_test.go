package cluster

import "testing"

func TestGossipDiagnosticsObserveCountsDegradedMessages(t *testing.T) {
	var d GossipDiagnostics

	d.observe("[DEBUG] memberlist: Initiating push/pull sync")
	d.observe("[ERR] memberlist: Failed UDP ping: write udp [::]:8946->10.0.0.2:8946: sendto: operation not permitted")

	snapshot := d.Snapshot()
	if snapshot.MessageTotal != 2 {
		t.Fatalf("MessageTotal = %d, want 2", snapshot.MessageTotal)
	}
	if snapshot.DegradedTotal != 1 {
		t.Fatalf("DegradedTotal = %d, want 1", snapshot.DegradedTotal)
	}
	if snapshot.LastDegraded == "" {
		t.Fatalf("LastDegraded is empty")
	}
	if snapshot.LastDegradedTime.IsZero() {
		t.Fatalf("LastDegradedTime is zero")
	}
}

func TestGossipDiagnosticsIgnoresEmptyMessages(t *testing.T) {
	var d GossipDiagnostics

	d.observe("")
	d.observe(" \n ")

	snapshot := d.Snapshot()
	if snapshot.MessageTotal != 0 || snapshot.DegradedTotal != 0 {
		t.Fatalf("snapshot = %+v, want zero counts", snapshot)
	}
}
