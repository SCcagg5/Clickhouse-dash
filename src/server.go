package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"


	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// embeddedStaticFiles contains the static frontend assets.
//
//go:embed static/*
var embeddedStaticFiles embed.FS

// dashboardServer owns HTTP handlers and shared dependencies.
type dashboardServer struct {
	logger *slog.Logger

	configuration applicationConfiguration

	clickhouseConnectionManager *clickhouseConnectionManager
	querySessionStore           *querySessionStore
}

// newDashboardServer creates a new dashboard server.
func newDashboardServer(
	logger *slog.Logger,
	configuration applicationConfiguration,
	clickhouseConnectionManager *clickhouseConnectionManager,
	querySessionStore *querySessionStore,
) *dashboardServer {
	return &dashboardServer{
		logger: logger,

		configuration: configuration,

		clickhouseConnectionManager: clickhouseConnectionManager,
		querySessionStore:           querySessionStore,
	}
}

// serve starts the HTTP server.
func (server *dashboardServer) serve() error {
	staticFilesSubFileSystem, subFileSystemError := fs.Sub(embeddedStaticFiles, "static")
	if subFileSystemError != nil {
		return subFileSystemError
	}


	multiplexer := http.NewServeMux()

	// Serve the UI at / and static assets at /static/*.
	multiplexer.HandleFunc("/", func(httpResponseWriter http.ResponseWriter, httpRequest *http.Request) {
		if httpRequest.URL.Path != "/" {
			http.NotFound(httpResponseWriter, httpRequest)
			return
		}

		file, err := staticFilesSubFileSystem.Open("index.html")
		if err != nil {
			http.Error(httpResponseWriter, "index.html not found", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		httpResponseWriter.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.Copy(httpResponseWriter, file)
	})

	multiplexer.Handle(
		"/static/",
		http.StripPrefix("/static/", http.FileServer(http.FS(staticFilesSubFileSystem))),
	)

	// API endpoints.
	multiplexer.Handle("/api/query", http.HandlerFunc(server.handleCreateQuery))
	multiplexer.Handle("/api/query/stream", http.HandlerFunc(server.handleQueryStream))
	multiplexer.Handle("/api/query/cancel", http.HandlerFunc(server.handleCancelQuery))

	// Health check.
	multiplexer.Handle("/healthz", http.HandlerFunc(func(httpResponseWriter http.ResponseWriter, httpRequest *http.Request) {
		writeJson(httpResponseWriter, http.StatusOK, map[string]any{"status": "ok"})
	}))

	httpServer := &http.Server{
		Addr:              server.configuration.serverListenAddress,
		Handler:           multiplexer,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      0, // SSE and long queries require long-lived responses.
		IdleTimeout:       60 * time.Second,
	}

	server.logger.Info("http server listening", "address", server.configuration.serverListenAddress)
	return httpServer.ListenAndServe()
}

// createQueryRequest is the request payload for POST /api/query.
type createQueryRequest struct {
	QueryText    string         `json:"sql"`
	DatabaseName string         `json:"database,omitempty"`
	Settings     map[string]any `json:"settings,omitempty"`
}

// handleCreateQuery handles POST /api/query.
//
// Request body JSON:
//   { "sql": "...", "database": "optional", "settings": { "max_execution_time": 10, ... } }
//
// Response JSON:
//   { "query_id": "...", "stream_url": "/api/query/stream?query_id=..." }
func (server *dashboardServer) handleCreateQuery(httpResponseWriter http.ResponseWriter, httpRequest *http.Request) {
	if httpRequest.Method != http.MethodPost {
		writeErrorJson(httpResponseWriter, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST is allowed.")
		return
	}

	requestBodyBytesReader := http.MaxBytesReader(httpResponseWriter, httpRequest.Body, 1<<20)
	defer requestBodyBytesReader.Close()

	var requestPayload createQueryRequest
	decoder := json.NewDecoder(requestBodyBytesReader)
	decoder.DisallowUnknownFields()
	if decodeError := decoder.Decode(&requestPayload); decodeError != nil {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "invalid_json", decodeError.Error())
		return
	}

	queryText := strings.TrimSpace(requestPayload.QueryText)
	if queryText == "" {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "missing_sql", "Field 'sql' must not be empty.")
		return
	}

	if !isReadOnlyClickhouseStatement(queryText) {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "forbidden_statement", "Only read-only statements are allowed in this MVP (SELECT/WITH/SHOW/DESCRIBE/DESC/EXPLAIN).")
		return
	}

	queryIdentifier, identifierError := generateQueryIdentifier()
	if identifierError != nil {
		writeErrorJson(httpResponseWriter, http.StatusInternalServerError, "server_error", "Unable to generate query identifier.")
		return
	}

	databaseName := strings.TrimSpace(requestPayload.DatabaseName)
	if databaseName == "" {
		databaseName = server.configuration.clickhouseDatabaseName
	}

	settings := buildDefaultQuerySettings(server.configuration)
	for key, value := range requestPayload.Settings {
		settings[key] = value
	}

	session := newQuerySession(server.logger, queryIdentifier, queryText, databaseName, settings)
	server.querySessionStore.create(session)

	writeJson(httpResponseWriter, http.StatusOK, map[string]any{
		"query_id":   queryIdentifier,
		"stream_url": "/api/query/stream?query_id=" + queryIdentifier,
	})
}

// buildDefaultQuerySettings returns ClickHouse settings applied to every query by default.
func buildDefaultQuerySettings(configuration applicationConfiguration) clickhouse.Settings {
	return clickhouse.Settings{
		// Server-side enforcement.
		"readonly": 1,

		// Basic guardrails for an MVP dashboard.
		"max_execution_time": configuration.defaultMaximumExecutionTimeSeconds,
		"max_result_rows":    configuration.defaultMaximumResultRows,
		"max_result_bytes":   configuration.defaultMaximumResultBytes,
	}
}

// cancelQueryRequest is the request payload for POST /api/query/cancel.
type cancelQueryRequest struct {
	QueryIdentifier string `json:"query_id"`
}

// handleCancelQuery handles POST /api/query/cancel.
//
// Request body JSON:
//   { "query_id": "..." }
func (server *dashboardServer) handleCancelQuery(httpResponseWriter http.ResponseWriter, httpRequest *http.Request) {
	if httpRequest.Method != http.MethodPost {
		writeErrorJson(httpResponseWriter, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST is allowed.")
		return
	}

	requestBodyBytesReader := http.MaxBytesReader(httpResponseWriter, httpRequest.Body, 1<<20)
	defer requestBodyBytesReader.Close()

	var requestPayload cancelQueryRequest
	decoder := json.NewDecoder(requestBodyBytesReader)
	decoder.DisallowUnknownFields()
	if decodeError := decoder.Decode(&requestPayload); decodeError != nil {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "invalid_json", decodeError.Error())
		return
	}

	queryIdentifier := strings.TrimSpace(requestPayload.QueryIdentifier)
	if queryIdentifier == "" {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "missing_query_id", "Field 'query_id' must not be empty.")
		return
	}

	session, exists := server.querySessionStore.get(queryIdentifier)
	if !exists {
		writeErrorJson(httpResponseWriter, http.StatusNotFound, "not_found", "Unknown query_id.")
		return
	}

	session.requestCancellation()

	// Best effort: also execute KILL QUERY using the administrator connection if possible.
	go server.killQueryBestEffort(queryIdentifier, session.databaseName)

	writeJson(httpResponseWriter, http.StatusOK, map[string]any{
		"query_id": queryIdentifier,
		"status":   "cancel_requested",
	})
}

// killQueryBestEffort attempts to stop a running query using KILL QUERY.
// If the server cancels the query via context cancellation, this is usually unnecessary, but it is a useful fallback.
func (server *dashboardServer) killQueryBestEffort(queryIdentifier string, databaseName string) {
	connection, connectionError := server.clickhouseConnectionManager.administratorConnectionForControlPlane(databaseName)
	if connectionError != nil {
		server.logger.Warn("unable to open administrator connection for KILL QUERY", "query_identifier", queryIdentifier, "error", connectionError.Error())
		return
	}

	if !isSafeQueryIdentifier(queryIdentifier) {
		server.logger.Warn("refusing to execute KILL QUERY: unsafe query identifier", "query_identifier", queryIdentifier)
		return
	}

	requestContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	killStatement := fmt.Sprintf("KILL QUERY WHERE query_id = '%s' SYNC", queryIdentifier)
	executionError := connection.Exec(requestContext, killStatement)
	if executionError != nil {
		server.logger.Warn("KILL QUERY failed", "query_identifier", queryIdentifier, "error", executionError.Error())
		return
	}
	server.logger.Info("KILL QUERY executed", "query_identifier", queryIdentifier)
}

// handleQueryStream handles GET /api/query/stream?query_id=... and streams SSE events.
func (server *dashboardServer) handleQueryStream(httpResponseWriter http.ResponseWriter, httpRequest *http.Request) {
	if httpRequest.Method != http.MethodGet {
		writeErrorJson(httpResponseWriter, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET is allowed.")
		return
	}

	queryIdentifier := strings.TrimSpace(httpRequest.URL.Query().Get("query_id"))
	if queryIdentifier == "" {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "missing_query_id", "Missing query_id query parameter.")
		return
	}

	session, exists := server.querySessionStore.get(queryIdentifier)
	if !exists {
		writeErrorJson(httpResponseWriter, http.StatusNotFound, "not_found", "Unknown query_id.")
		return
	}

	flusher, supportsFlushing := httpResponseWriter.(http.Flusher)
	if !supportsFlushing {
		writeErrorJson(httpResponseWriter, http.StatusInternalServerError, "server_error", "ResponseWriter does not support flushing (required for SSE).")
		return
	}

	// SSE response headers.
	httpResponseWriter.Header().Set("Content-Type", "text/event-stream")
	httpResponseWriter.Header().Set("Cache-Control", "no-cache")
	httpResponseWriter.Header().Set("Connection", "keep-alive")
	httpResponseWriter.Header().Set("X-Accel-Buffering", "no")

	// Initial meta event.
	_ = writeServerSentEvent(httpResponseWriter, flusher, "meta", map[string]any{
		"query_id": queryIdentifier,
		"status":   "connected",
	})

	clickhouseConnection, connectionError := server.clickhouseConnectionManager.connectionForDatabase(session.databaseName)
	if connectionError != nil {
		_ = writeServerSentEvent(httpResponseWriter, flusher, "error", buildErrorPayload(queryIdentifier, connectionError))
		_ = writeServerSentEvent(httpResponseWriter, flusher, "done", map[string]any{
			"query_id": queryIdentifier,
			"status":   "error",
			"message":  connectionError.Error(),
		})
		return
	}

	session.start(clickhouseConnection, server.querySessionStore)

	// If the browser disconnects, cancel the query (best effort).
	go func() {
		<-httpRequest.Context().Done()
		session.requestCancellation()
	}()

	metricsTicker := time.NewTicker(500 * time.Millisecond) // 4 Hz
	defer metricsTicker.Stop()

	keepAliveTicker := time.NewTicker(15 * time.Second)
	defer keepAliveTicker.Stop()

	var previousSnapshotTime time.Time
	var previousReadRows uint64
	var previousReadBytes uint64
	var previousCentralProcessingUnitSeconds float64

	for {
		select {
		case nonCriticalMessage := <-session.nonCriticalEventChannel:
			_ = writeServerSentEvent(httpResponseWriter, flusher, nonCriticalMessage.eventName, nonCriticalMessage.payload)

		case criticalMessage := <-session.criticalEventChannel:
			_ = writeServerSentEvent(httpResponseWriter, flusher, criticalMessage.eventName, criticalMessage.payload)
			if criticalMessage.eventName == "done" {
				return
			}

		case now := <-metricsTicker.C:
			sessionSnapshot := session.snapshot(now)

			if sessionSnapshot.status == querySessionStatusCreated {
				continue
			}

			elapsedSeconds := 0.0
			if !sessionSnapshot.startedTime.IsZero() {
				elapsedSeconds = now.Sub(sessionSnapshot.startedTime).Seconds()
			}

			centralProcessingUnitSeconds := float64(sessionSnapshot.userTimeMicrosecondsTotal+sessionSnapshot.systemTimeMicrosecondsTotal) / 1e6

			rowsPerSecondTotal := safeRate(float64(sessionSnapshot.readRowsTotal), elapsedSeconds)
			bytesPerSecondTotal := safeRate(float64(sessionSnapshot.readBytesTotal), elapsedSeconds)

			progressPercent, progressPercentKnown := calculateProgressPercent(
				0,
				sessionSnapshot.readBytesTotal,
				sessionSnapshot.totalRowsToRead,
				sessionSnapshot.readRowsTotal,
			)

			instantRowsPerSecond := 0.0
			instantBytesPerSecond := 0.0
			instantCentralProcessingUnitCorePercent := 0.0

			if !previousSnapshotTime.IsZero() {
				sampleDurationSeconds := now.Sub(previousSnapshotTime).Seconds()
				readRowsDelta := float64(sessionSnapshot.readRowsTotal - previousReadRows)
				readBytesDelta := float64(sessionSnapshot.readBytesTotal - previousReadBytes)
				instantRowsPerSecond = safeRate(readRowsDelta, sampleDurationSeconds)
				instantBytesPerSecond = safeRate(readBytesDelta, sampleDurationSeconds)

				centralProcessingUnitSecondsDelta := centralProcessingUnitSeconds - previousCentralProcessingUnitSeconds
				instantCentralProcessingUnitCorePercent = safePercent(centralProcessingUnitSecondsDelta, sampleDurationSeconds)
			}

			totalCentralProcessingUnitCorePercent := safePercent(centralProcessingUnitSeconds, elapsedSeconds)

			previousSnapshotTime = now
			previousReadRows = sessionSnapshot.readRowsTotal
			previousReadBytes = sessionSnapshot.readBytesTotal
			previousCentralProcessingUnitSeconds = centralProcessingUnitSeconds

			threadCountCurrent, threadCountPeak := estimateThreadCounts(now, sessionSnapshot.threadLastSeenByIdentifier, 2*time.Second, sessionSnapshot.threadPeakCount)

			server.persistThreadPeakCount(session, threadCountPeak)

			progressEventPayload := map[string]any{
				"query_id":               sessionSnapshot.queryIdentifier,
				"elapsed_seconds":        elapsedSeconds,
				"percent":                progressPercent,
				"percent_known":          progressPercentKnown,
				"read_rows":              sessionSnapshot.readRowsTotal,
				"read_bytes":             sessionSnapshot.readBytesTotal,
				"total_rows_to_read":     sessionSnapshot.totalRowsToRead,
				"total_bytes_to_read":    0,
				"rows_per_second":        rowsPerSecondTotal,
				"bytes_per_second":       bytesPerSecondTotal,
				"rows_per_second_inst":   instantRowsPerSecond,
				"bytes_per_second_inst":  instantBytesPerSecond,
			}

			resourceEventPayload := map[string]any{
				"query_id": sessionSnapshot.queryIdentifier,

				"central_processing_unit_seconds":              centralProcessingUnitSeconds,
				"central_processing_unit_core_percent_total":   totalCentralProcessingUnitCorePercent,
				"central_processing_unit_core_percent_instant": instantCentralProcessingUnitCorePercent,

				"thread_count_current": threadCountCurrent,
				"thread_count_peak":    threadCountPeak,
			}

			if sessionSnapshot.currentMemoryBytes != nil {
				resourceEventPayload["memory_current_bytes"] = *sessionSnapshot.currentMemoryBytes
			} else {
				resourceEventPayload["memory_current_bytes"] = nil
			}
			if sessionSnapshot.peakMemoryBytes != nil {
				resourceEventPayload["memory_peak_bytes"] = *sessionSnapshot.peakMemoryBytes
			} else {
				resourceEventPayload["memory_peak_bytes"] = nil
			}

			_ = writeServerSentEvent(httpResponseWriter, flusher, "progress", progressEventPayload)
			_ = writeServerSentEvent(httpResponseWriter, flusher, "resource", resourceEventPayload)

			if sessionSnapshot.status == querySessionStatusFinished || sessionSnapshot.status == querySessionStatusErrored || sessionSnapshot.status == querySessionStatusCanceled {
				_ = writeServerSentEvent(httpResponseWriter, flusher, "done", map[string]any{
					"query_id":        sessionSnapshot.queryIdentifier,
					"status":          sessionSnapshot.status,
					"elapsed_seconds": elapsedSeconds,
					"read_rows":       sessionSnapshot.readRowsTotal,
					"read_bytes":      sessionSnapshot.readBytesTotal,
				})
				return
			}

		case <-keepAliveTicker.C:
			_, _ = fmt.Fprint(httpResponseWriter, ": keep-alive\n\n")
			flusher.Flush()

		case <-httpRequest.Context().Done():
			return
		}
	}
}

// persistThreadPeakCount stores the latest peak thread count in the session.
func (server *dashboardServer) persistThreadPeakCount(session *querySession, observedPeak int) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if observedPeak > session.threadPeakCount {
		session.threadPeakCount = observedPeak
	}
}

// writeServerSentEvent writes a single SSE event and flushes.
func writeServerSentEvent(httpResponseWriter http.ResponseWriter, flusher http.Flusher, eventName string, payload any) error {
	serializedPayloadBytes, marshalError := json.Marshal(payload)
	if marshalError != nil {
		return marshalError
	}

	_, _ = fmt.Fprintf(httpResponseWriter, "event: %s\n", eventName)
	_, _ = fmt.Fprintf(httpResponseWriter, "data: %s\n\n", string(serializedPayloadBytes))
	flusher.Flush()
	return nil
}
