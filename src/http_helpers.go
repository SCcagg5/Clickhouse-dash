package main

import (
	"encoding/json"
	"net/http"
)

// writeJson writes a JSON response with the provided status code.
func writeJson(httpResponseWriter http.ResponseWriter, statusCode int, payload any) {
	httpResponseWriter.Header().Set("Content-Type", "application/json; charset=utf-8")
	httpResponseWriter.WriteHeader(statusCode)

	encoder := json.NewEncoder(httpResponseWriter)
	encoder.SetEscapeHTML(true)
	_ = encoder.Encode(payload)
}

// writeErrorJson writes a JSON error response with a stable error code and human-readable message.
func writeErrorJson(httpResponseWriter http.ResponseWriter, statusCode int, code string, message string) {
	writeJson(httpResponseWriter, statusCode, map[string]any{
		"error":   code,
		"message": message,
	})
}
