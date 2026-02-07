package main

import "testing"

func TestIsReadOnlyClickhouseStatement(t *testing.T) {
	testCases := []struct {
		name             string
		statement        string
		expectedReadOnly bool
	}{
		{"simple select", "SELECT 1", true},
		{"leading spaces", "   \n\tSELECT 1", true},
		{"with select", "WITH 1 AS x SELECT x", true},
		{"show create", "SHOW CREATE TABLE system.numbers", true},
		{"describe table", "DESCRIBE TABLE system.numbers", true},
		{"desc table", "DESC TABLE system.numbers", true},
		{"explain", "EXPLAIN SELECT 1", true},

		{"insert forbidden", "INSERT INTO t VALUES (1)", false},
		{"create forbidden", "CREATE TABLE t (x UInt8) ENGINE=Memory", false},
		{"drop forbidden", "DROP TABLE t", false},
		{"set forbidden", "SET max_threads = 1", false},

		{"comment only", "-- hello\n", false},
		{"block comment only", "/* hello */", false},
		{"comment then select", "-- hello\nSELECT 1", true},
		{"block comment then select", "/* hello */ SELECT 1", true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := isReadOnlyClickhouseStatement(testCase.statement)
			if actual != testCase.expectedReadOnly {
				t.Fatalf("expected %v, got %v (statement=%q)", testCase.expectedReadOnly, actual, testCase.statement)
			}
		})
	}
}

func TestFirstKeywordInStatement(t *testing.T) {
	firstKeyword, ok := firstKeywordInStatement(" /*comment*/ \n -- hello\n  SELECT x")
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if firstKeyword != "SELECT" {
		t.Fatalf("expected SELECT, got %q", firstKeyword)
	}

	_, ok = firstKeywordInStatement("-- only comment")
	if ok {
		t.Fatalf("expected ok=false for comment-only statement")
	}
}
