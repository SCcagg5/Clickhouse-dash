package main

import (
	"errors"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// buildErrorPayload normalizes ClickHouse (and generic) errors into a stable JSON shape for SSE.
func buildErrorPayload(queryIdentifier string, executionError error) map[string]any {
	payload := map[string]any{
		"query_id": queryIdentifier,
		"message":  executionError.Error(),
	}

	var clickhouseException *clickhouse.Exception
	if errors.As(executionError, &clickhouseException) {
		payload["code"] = clickhouseException.Code
		payload["exception_message"] = clickhouseException.Message
		if clickhouseException.StackTrace != "" {
			payload["stack_trace"] = truncateForLogs(clickhouseException.StackTrace, 4000)
		}
	}

	return payload
}
