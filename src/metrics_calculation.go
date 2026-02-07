package main

import (
	"math"
	"time"
)

// safeRate calculates value/seconds, returning 0 when seconds is <= 0.
func safeRate(value float64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return value / seconds
}

// safePercent calculates (numerator/denominator)*100, returning 0 when denominator is <= 0.
func safePercent(numerator float64, denominator float64) float64 {
	if denominator <= 0 {
		return 0
	}
	return (numerator / denominator) * 100
}

// calculateProgressPercent implements the progress percentage rules described in the requirements.
//
// Priority:
//   1) If totalBytesToRead > 0: percent = readBytes / totalBytesToRead * 100
//   2) Else if totalRowsToRead > 0: percent = readRows / totalRowsToRead * 100
//   3) Else: percent is unknown (indeterminate).
//
// The returned percent is clamped to [0, 100].
func calculateProgressPercent(totalBytesToRead uint64, readBytes uint64, totalRowsToRead uint64, readRows uint64) (float64, bool) {
	if totalBytesToRead > 0 {
		return clampToZeroToOneHundred((float64(readBytes) / float64(totalBytesToRead)) * 100), true
	}
	if totalRowsToRead > 0 {
		return clampToZeroToOneHundred((float64(readRows) / float64(totalRowsToRead)) * 100), true
	}
	return 0, false
}

// clampToZeroToOneHundred clamps a float64 to [0, 100].
func clampToZeroToOneHundred(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

// estimateThreadCounts estimates current and peak thread counts from profile events.
//
// Because ClickHouse does not expose "current threads of a running query" as a stable per-query gauge
// in the native protocol, we infer it from the set of thread identifiers that produced profile events
// recently. This is a best-effort approximation.
func estimateThreadCounts(
	now time.Time,
	lastSeenByThreadIdentifier map[uint64]time.Time,
	activityWindow time.Duration,
	previouslyObservedPeak int,
) (int, int) {
	currentCount := 0
	for _, lastSeen := range lastSeenByThreadIdentifier {
		if now.Sub(lastSeen) <= activityWindow {
			currentCount++
		}
	}

	peakCount := previouslyObservedPeak
	if currentCount > peakCount {
		peakCount = currentCount
	}
	return currentCount, peakCount
}
