// ClickHouse Dashboard MVP (TCP + SSE)
//
// This program implements a minimal ClickHouse dashboard focused on query execution telemetry.
// Queries are executed via the ClickHouse native TCP protocol (using the official Go client),
// and progress/profile updates are pushed to the browser using Server-Sent Events (EventSource).
//
// The UI is intentionally minimal and the backend is "read-only by default".
package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	configuration, configurationError := loadApplicationConfiguration()
	if configurationError != nil {
		logger.Error("configuration error", "error", configurationError.Error())
		os.Exit(1)
	}

	clickhouseConnectionManager := newClickhouseConnectionManager(
		logger,
		configuration.clickhouseAddressOrDsn,
		configuration.clickhouseUserName,
		configuration.clickhousePassword,
		configuration.clickhouseDialTimeout,
		configuration.clickhouseAdministratorUserName,
		configuration.clickhouseAdministratorPassword,
	)
	defer clickhouseConnectionManager.closeAll()

	querySessionStore := newQuerySessionStore(logger, configuration)

	dashboardServer := newDashboardServer(
		logger,
		configuration,
		clickhouseConnectionManager,
		querySessionStore,
	)

	serveError := dashboardServer.serve()
	if serveError != nil && !errors.Is(serveError, http.ErrServerClosed) {
		logger.Error("server stopped with error", "error", serveError.Error())
		os.Exit(1)
	}
}
