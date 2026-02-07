package main

import "strings"

// isReadOnlyClickhouseStatement applies a simple "MVP read-only" guard.
// It allows statements starting with: SELECT, WITH, SHOW, DESCRIBE, DESC, EXPLAIN.
//
// This is a best-effort guard. For strong enforcement, this dashboard also sets the ClickHouse
// setting "readonly=1" on every executed query.
func isReadOnlyClickhouseStatement(statement string) bool {
	firstKeyword, ok := firstKeywordInStatement(statement)
	if !ok {
		return false
	}

	switch strings.ToUpper(firstKeyword) {
	case "SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN":
		return true
	default:
		return false
	}
}

// firstKeywordInStatement extracts the first SQL keyword, skipping whitespace and comments.
// It supports ClickHouse-style line comments ("--") and block comments ("/* ... */").
func firstKeywordInStatement(statement string) (string, bool) {
	remaining := strings.TrimSpace(statement)
	if remaining == "" {
		return "", false
	}

	// Remove leading SQL comments: -- ... \n and /* ... */
	for {
		remaining = strings.TrimLeft(remaining, " \t\r\n")
		if strings.HasPrefix(remaining, "--") {
			newlineIndex := strings.Index(remaining, "\n")
			if newlineIndex == -1 {
				return "", false
			}
			remaining = remaining[newlineIndex+1:]
			continue
		}
		if strings.HasPrefix(remaining, "/*") {
			endIndex := strings.Index(remaining, "*/")
			if endIndex == -1 {
				return "", false
			}
			remaining = remaining[endIndex+2:]
			continue
		}
		break
	}

	remaining = strings.TrimLeft(remaining, " \t\r\n")
	if remaining == "" {
		return "", false
	}

	endIndex := 0
	for endIndex < len(remaining) {
		character := remaining[endIndex]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_' {
			endIndex++
			continue
		}
		break
	}
	if endIndex == 0 {
		return "", false
	}
	return remaining[:endIndex], true
}
