package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// clickhouseConnectionManager provides ClickHouse connections keyed by database name.
//
// A single ClickHouse connection object can internally manage multiple connections. However,
// this dashboard supports an optional per-query database override, so we keep a small map
// keyed by database name and lazily create connections as needed.
type clickhouseConnectionManager struct {
	logger *slog.Logger

	addressOrDsn string
	userName     string
	password     string

	dialTimeout time.Duration

	mutex                sync.Mutex
	connectionsByDatabase map[string]clickhouseDriver.Conn

	administratorConnection      clickhouseDriver.Conn
	administratorConnectionError error
	administratorUserName        string
	administratorPassword        string
}

// newClickhouseConnectionManager creates a new connection manager.
func newClickhouseConnectionManager(
	logger *slog.Logger,
	addressOrDsn string,
	userName string,
	password string,
	dialTimeout time.Duration,
	administratorUserName string,
	administratorPassword string,
) *clickhouseConnectionManager {
	return &clickhouseConnectionManager{
		logger: logger,

		addressOrDsn: addressOrDsn,
		userName:     userName,
		password:     password,
		dialTimeout:  dialTimeout,

		connectionsByDatabase: make(map[string]clickhouseDriver.Conn),

		administratorUserName: administratorUserName,
		administratorPassword: administratorPassword,
	}
}

// connectionForDatabase returns (and lazily creates) a ClickHouse connection for the given database name.
func (manager *clickhouseConnectionManager) connectionForDatabase(databaseName string) (clickhouseDriver.Conn, error) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if existingConnection, exists := manager.connectionsByDatabase[databaseName]; exists {
		return existingConnection, nil
	}

	newConnection, openError := manager.openConnection(databaseName, manager.userName, manager.password)
	if openError != nil {
		return nil, openError
	}

	manager.connectionsByDatabase[databaseName] = newConnection
	return newConnection, nil
}

// administratorConnectionForControlPlane returns a connection for administrative operations such as KILL QUERY.
// If CH_ADMIN_USER/CH_ADMIN_PASS are not configured, it falls back to the regular user credentials.
func (manager *clickhouseConnectionManager) administratorConnectionForControlPlane(databaseName string) (clickhouseDriver.Conn, error) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.administratorConnection != nil || manager.administratorConnectionError != nil {
		if manager.administratorConnection != nil {
			return manager.administratorConnection, nil
		}
		return nil, manager.administratorConnectionError
	}

	effectiveAdministratorUserName := manager.userName
	effectiveAdministratorPassword := manager.password
	if manager.administratorUserName != "" {
		effectiveAdministratorUserName = manager.administratorUserName
		effectiveAdministratorPassword = manager.administratorPassword
	}

	newConnection, openError := manager.openConnection(databaseName, effectiveAdministratorUserName, effectiveAdministratorPassword)
	if openError != nil {
		manager.administratorConnectionError = openError
		return nil, openError
	}

	manager.administratorConnection = newConnection
	return manager.administratorConnection, nil
}

// closeAll closes all open ClickHouse connections.
func (manager *clickhouseConnectionManager) closeAll() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	for databaseName, existingConnection := range manager.connectionsByDatabase {
		_ = existingConnection.Close()
		delete(manager.connectionsByDatabase, databaseName)
	}

	if manager.administratorConnection != nil {
		_ = manager.administratorConnection.Close()
		manager.administratorConnection = nil
	}
}

func (manager *clickhouseConnectionManager) invalidateDatabaseConnection(databaseName string) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if existing, ok := manager.connectionsByDatabase[databaseName]; ok {
		_ = existing.Close()
		delete(manager.connectionsByDatabase, databaseName)
	}
}

// openConnection opens a ClickHouse connection with the provided database and credentials.
func (manager *clickhouseConnectionManager) openConnection(databaseName string, userName string, password string) (clickhouseDriver.Conn, error) {
	if strings.TrimSpace(manager.addressOrDsn) == "" {
		return nil, errors.New("CH_URL is empty")
	}

	if strings.Contains(manager.addressOrDsn, "://") {
		parsedOptions, parseError := clickhouse.ParseDSN(manager.addressOrDsn)
		if parseError != nil {
			return nil, fmt.Errorf("failed to parse ClickHouse DSN: %w", parseError)
		}

		parsedOptions.Auth.Database = databaseName

		conn, openError := clickhouse.Open(parsedOptions)
		if openError != nil {
			return nil, fmt.Errorf("failed to open ClickHouse connection (dsn): %w", openError)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if pingError := conn.Ping(ctx); pingError != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("clickhouse ping failed: %w", pingError)
		}

		return conn, nil
	}

	addresses := parseCommaSeparatedAddresses(manager.addressOrDsn)
	options := &clickhouse.Options{
		Addr: addresses,
		Auth: clickhouse.Auth{
			Database: databaseName,
			Username: userName,
			Password: password,
		},
		DialContext: manager.dialContext(),
		Protocol:    clickhouse.Native,
	}

	connection, openError := clickhouse.Open(options)
	if openError != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", openError)
	}

	// Verify connectivity early.
	requestContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if pingError := connection.Ping(requestContext); pingError != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("clickhouse ping failed: %w", pingError)
	}

	return connection, nil
}

// dialContext returns a context-aware dial function with the configured timeout.
func (manager *clickhouseConnectionManager) dialContext() func(requestContext context.Context, address string) (net.Conn, error) {
	return func(requestContext context.Context, address string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: manager.dialTimeout}
		return dialer.DialContext(requestContext, "tcp", address)
	}
}

// parseCommaSeparatedAddresses splits comma-separated addresses and trims spaces.
func parseCommaSeparatedAddresses(commaSeparatedAddresses string) []string {
	rawParts := strings.Split(commaSeparatedAddresses, ",")
	addresses := make([]string, 0, len(rawParts))
	for _, rawPart := range rawParts {
		address := strings.TrimSpace(rawPart)
		if address == "" {
			continue
		}
		addresses = append(addresses, address)
	}
	return addresses
}
