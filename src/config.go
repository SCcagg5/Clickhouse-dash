package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// applicationConfiguration contains all runtime configuration loaded from environment variables.
type applicationConfiguration struct {
	serverListenAddress string

	clickhouseAddressOrDsn string
	clickhouseUserName     string
	clickhousePassword     string
	clickhouseDatabaseName string

	clickhouseAdministratorUserName string
	clickhouseAdministratorPassword string

	clickhouseDialTimeout time.Duration

	defaultMaximumExecutionTimeSeconds int
	defaultMaximumResultRows           int
	defaultMaximumResultBytes          int

	sessionExpirationIfNotStarted time.Duration
	sessionExpirationAfterFinish  time.Duration
}

// loadApplicationConfiguration reads configuration from environment variables and returns a validated configuration.
func loadApplicationConfiguration() (applicationConfiguration, error) {
	listenHost := strings.TrimSpace(
		getEnvironmentVariableOrDefault("LISTEN_HOST", "127.0.0.1"),
	)

	listenPort := strings.TrimSpace(
		getEnvironmentVariableOrDefault("LISTEN_PORT", "8080"),
	)

	serverListenAddress := fmt.Sprintf("%s:%s", listenHost, listenPort)

	clickhouseAddressOrDsn := strings.TrimSpace(getEnvironmentVariableOrDefault("CH_URL", "127.0.0.1:9000"))
	clickhouseUserName := strings.TrimSpace(getEnvironmentVariableOrDefault("CH_USER", "default"))
	clickhousePassword := getEnvironmentVariableOrDefault("CH_PASS", "")
	clickhouseDatabaseName := strings.TrimSpace(getEnvironmentVariableOrDefault("CH_DATABASE", "default"))

	clickhouseAdministratorUserName := strings.TrimSpace(getEnvironmentVariableOrDefault("CH_ADMIN_USER", ""))
	clickhouseAdministratorPassword := getEnvironmentVariableOrDefault("CH_ADMIN_PASS", "")

	clickhouseDialTimeout, clickhouseDialTimeoutError := time.ParseDuration(strings.TrimSpace(getEnvironmentVariableOrDefault("CH_DIAL_TIMEOUT", "5s")))
	if clickhouseDialTimeoutError != nil {
		return applicationConfiguration{}, fmt.Errorf("invalid CH_DIAL_TIMEOUT: %w", clickhouseDialTimeoutError)
	}

	defaultMaximumExecutionTimeSeconds, defaultMaximumExecutionTimeSecondsError := parseNonNegativeIntegerEnvironmentVariable("DEFAULT_MAX_EXECUTION_SECONDS", 60)
	if defaultMaximumExecutionTimeSecondsError != nil {
		return applicationConfiguration{}, defaultMaximumExecutionTimeSecondsError
	}
	defaultMaximumResultRows, defaultMaximumResultRowsError := parseNonNegativeIntegerEnvironmentVariable("DEFAULT_MAX_RESULT_ROWS", 5000)
	if defaultMaximumResultRowsError != nil {
		return applicationConfiguration{}, defaultMaximumResultRowsError
	}
	defaultMaximumResultBytes, defaultMaximumResultBytesError := parseNonNegativeIntegerEnvironmentVariable("DEFAULT_MAX_RESULT_BYTES", 50*1024*1024)
	if defaultMaximumResultBytesError != nil {
		return applicationConfiguration{}, defaultMaximumResultBytesError
	}

	sessionExpirationIfNotStarted, sessionExpirationIfNotStartedError := time.ParseDuration(strings.TrimSpace(getEnvironmentVariableOrDefault("SESSION_EXPIRE_IF_NOT_STARTED", "15m")))
	if sessionExpirationIfNotStartedError != nil {
		return applicationConfiguration{}, fmt.Errorf("invalid SESSION_EXPIRE_IF_NOT_STARTED: %w", sessionExpirationIfNotStartedError)
	}
	sessionExpirationAfterFinish, sessionExpirationAfterFinishError := time.ParseDuration(strings.TrimSpace(getEnvironmentVariableOrDefault("SESSION_EXPIRE_AFTER_FINISH", "2m")))
	if sessionExpirationAfterFinishError != nil {
		return applicationConfiguration{}, fmt.Errorf("invalid SESSION_EXPIRE_AFTER_FINISH: %w", sessionExpirationAfterFinishError)
	}

	return applicationConfiguration{
		serverListenAddress: serverListenAddress,

		clickhouseAddressOrDsn: clickhouseAddressOrDsn,
		clickhouseUserName:     clickhouseUserName,
		clickhousePassword:     clickhousePassword,
		clickhouseDatabaseName: clickhouseDatabaseName,

		clickhouseAdministratorUserName: clickhouseAdministratorUserName,
		clickhouseAdministratorPassword: clickhouseAdministratorPassword,

		clickhouseDialTimeout: clickhouseDialTimeout,

		defaultMaximumExecutionTimeSeconds: defaultMaximumExecutionTimeSeconds,
		defaultMaximumResultRows:           defaultMaximumResultRows,
		defaultMaximumResultBytes:          defaultMaximumResultBytes,

		sessionExpirationIfNotStarted: sessionExpirationIfNotStarted,
		sessionExpirationAfterFinish:  sessionExpirationAfterFinish,
	}, nil
}

// getEnvironmentVariableOrDefault returns the environment variable value if set, otherwise the provided default.
func getEnvironmentVariableOrDefault(environmentVariableName string, defaultValue string) string {
	value, exists := os.LookupEnv(environmentVariableName)
	if !exists {
		return defaultValue
	}
	return value
}

// parseNonNegativeIntegerEnvironmentVariable parses an integer environment variable with a default value.
// It returns an error if the value is not an integer or is negative.
func parseNonNegativeIntegerEnvironmentVariable(environmentVariableName string, defaultValue int) (int, error) {
	rawValue := strings.TrimSpace(getEnvironmentVariableOrDefault(environmentVariableName, strconv.Itoa(defaultValue)))
	parsedValue, parseError := strconv.Atoi(rawValue)
	if parseError != nil {
		return 0, fmt.Errorf("invalid %s: %w", environmentVariableName, parseError)
	}
	if parsedValue < 0 {
		return 0, fmt.Errorf("invalid %s: must be >= 0", environmentVariableName)
	}
	return parsedValue, nil
}
