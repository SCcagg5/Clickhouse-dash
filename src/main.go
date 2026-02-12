package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	healthMode := flag.Bool("health", false, "Run health probe and exit (0=healthy, 1=unhealthy)")
	flag.Parse()

	// If called as health probe, don't boot the full server.
	if *healthMode {
		if err := runHealthProbe(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

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

// runHealthProbe checks the local HTTP endpoint /healthz.
// It assumes your /healthz checks ClickHouse connectivity (readiness).
func runHealthProbe() error {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}

	url := "http://127.0.0.1:" + port + "/healthz"

	// tight timeouts so healthchecks don't pile up
	dialer := &net.Dialer{Timeout: 300 * time.Millisecond}
	client := &http.Client{
		Timeout: 900 * time.Millisecond,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   300 * time.Millisecond,
			ResponseHeaderTimeout: 600 * time.Millisecond,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 950*time.Millisecond)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("health probe failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health probe failed: %s", resp.Status)
	}
	return nil
}
