package main

import (
	"regexp"
	"strconv"
	"strings"
)

var peakMemoryUsageRegularExpression = regexp.MustCompile(`(?i)\bpeak\s+memory\s+usage\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?i?B)\b`)

// parsePeakMemoryUsageFromLogLine parses log messages like "Peak memory usage: 123.45 MiB" and returns bytes.
func parsePeakMemoryUsageFromLogLine(logLine string) (uint64, bool) {
	matches := peakMemoryUsageRegularExpression.FindStringSubmatch(logLine)
	if len(matches) != 3 {
		return 0, false
	}

	numberText := matches[1]
	unitText := matches[2]

	numberValue, parseError := strconv.ParseFloat(numberText, 64)
	if parseError != nil {
		return 0, false
	}

	multiplier := humanSizeUnitMultiplier(unitText)
	if multiplier == 0 {
		return 0, false
	}

	bytes := numberValue * float64(multiplier)
	if bytes < 0 {
		return 0, false
	}
	return uint64(bytes), true
}

// humanSizeUnitMultiplier returns the multiplier for units like B, KiB, MiB, GiB, TiB.
func humanSizeUnitMultiplier(unitText string) uint64 {
	switch strings.ToUpper(unitText) {
	case "B":
		return 1
	case "KB":
		return 1000
	case "MB":
		return 1000 * 1000
	case "GB":
		return 1000 * 1000 * 1000
	case "TB":
		return 1000 * 1000 * 1000 * 1000
	case "KIB":
		return 1024
	case "MIB":
		return 1024 * 1024
	case "GIB":
		return 1024 * 1024 * 1024
	case "TIB":
		return 1024 * 1024 * 1024 * 1024
	default:
		return 0
	}
}
