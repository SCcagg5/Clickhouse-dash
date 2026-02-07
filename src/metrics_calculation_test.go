package main

import (
	"testing"
	"time"
)

func TestCalculateProgressPercent(t *testing.T) {
	percent, known := calculateProgressPercent(0, 0, 0, 0)
	if known {
		t.Fatalf("expected percent to be unknown")
	}
	if percent != 0 {
		t.Fatalf("expected percent=0, got %v", percent)
	}

	percent, known = calculateProgressPercent(0, 0, 100, 25)
	if !known {
		t.Fatalf("expected percent to be known")
	}
	if percent != 25 {
		t.Fatalf("expected 25, got %v", percent)
	}

	percent, known = calculateProgressPercent(1000, 250, 0, 0)
	if !known {
		t.Fatalf("expected percent to be known")
	}
	if percent != 25 {
		t.Fatalf("expected 25, got %v", percent)
	}

	percent, known = calculateProgressPercent(100, 1000, 0, 0)
	if !known {
		t.Fatalf("expected percent to be known")
	}
	if percent != 100 {
		t.Fatalf("expected clamp to 100, got %v", percent)
	}
}

func TestEstimateThreadCounts(t *testing.T) {
	now := time.Now()
	lastSeenByThreadIdentifier := map[uint64]time.Time{
		1: now.Add(-500 * time.Millisecond),
		2: now.Add(-3 * time.Second),
		3: now.Add(-1 * time.Second),
	}

	currentCount, peakCount := estimateThreadCounts(now, lastSeenByThreadIdentifier, 2*time.Second, 4)
	if currentCount != 2 {
		t.Fatalf("expected currentCount=2, got %d", currentCount)
	}
	if peakCount != 4 {
		t.Fatalf("expected peakCount=4, got %d", peakCount)
	}

	currentCount, peakCount = estimateThreadCounts(now, lastSeenByThreadIdentifier, 2*time.Second, 1)
	if peakCount != 2 {
		t.Fatalf("expected peakCount to update to 2, got %d", peakCount)
	}
}
