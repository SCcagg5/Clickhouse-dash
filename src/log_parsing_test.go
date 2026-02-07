package main

import "testing"

func TestParsePeakMemoryUsageFromLogLine(t *testing.T) {
	bytes, ok := parsePeakMemoryUsageFromLogLine("Peak memory usage: 1.50 MiB")
	if !ok {
		t.Fatalf("expected ok=true")
	}
	expected := uint64(1.5 * 1024 * 1024)
	if bytes != expected {
		t.Fatalf("expected %d, got %d", expected, bytes)
	}

	_, ok = parsePeakMemoryUsageFromLogLine("some unrelated log text")
	if ok {
		t.Fatalf("expected ok=false")
	}
}
