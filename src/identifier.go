package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// generateQueryIdentifier returns a random identifier suitable for ClickHouse query_id.
func generateQueryIdentifier() (string, error) {
	randomBytes := make([]byte, 16)
	_, randomReadError := rand.Read(randomBytes)
	if randomReadError != nil {
		return "", randomReadError
	}

	// Format as UUID v4 (RFC 4122).
	randomBytes[6] = (randomBytes[6] & 0x0f) | 0x40
	randomBytes[8] = (randomBytes[8] & 0x3f) | 0x80

	hexString := hex.EncodeToString(randomBytes)
	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		hexString[0:8],
		hexString[8:12],
		hexString[12:16],
		hexString[16:20],
		hexString[20:32],
	), nil
}

// truncateForLogs returns a shortened preview suitable for logs.
func truncateForLogs(text string, maximumLength int) string {
	if maximumLength <= 0 {
		return ""
	}
	if len(text) <= maximumLength {
		return text
	}
	return text[:maximumLength] + "..."
}

// isSafeQueryIdentifier checks whether a query identifier is safe to embed into SQL.
// This avoids SQL injection in the KILL QUERY best-effort fallback.
func isSafeQueryIdentifier(queryIdentifier string) bool {
	for _, character := range queryIdentifier {
		if (character >= 'a' && character <= 'f') ||
			(character >= 'A' && character <= 'F') ||
			(character >= '0' && character <= '9') ||
			character == '-' {
			continue
		}
		return false
	}
	return true
}
