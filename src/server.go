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
func (server *dashboardServer) handleCreateQuery(httpResponseWriter http.ResponseWriter, httpRequest *http.Request) {
	if httpRequest.Method != http.MethodPost {
		writeErrorJson(httpResponseWriter, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST is allowed.")
		return
	}

	requestBodyBytesReader := http.MaxBytesReader(httpResponseWriter, httpRequest.Body, 1<<20)
	defer requestBodyBytesReader.Close()

	var requestPayload createQueryRequest
	decodeError := json.NewDecoder(requestBodyBytesReader).Decode(&requestPayload)
	if decodeError != nil {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "invalid_json", "Invalid JSON request body.")
		return
	}

	queryText := strings.TrimSpace(requestPayload.QueryText)
	if queryText == "" {
		writeErrorJson(httpResponseWriter, http.StatusBadRequest, "missing_sql", "Missing SQL text.")
		return
	}

	databaseName := strings.TrimSpace(requestPayload.DatabaseName)
	if databaseName == "" {
		databaseName = server.configuration.clickhouseDatabaseName
	}

	queryIdentifier, identifierError := generateQueryIdentifier()
	if identifierError != nil {
		writeErrorJson(httpResponseWriter, http.StatusInternalServerError, "server_error", "Failed to generate query identifier.")
		return
	}

	settings := buildDefaultQuerySettings(server.configuration)
	for settingKey, settingValue := range requestPayload.Settings {
		settings[settingKey] = settingValue
	}

	session := newQuerySession(
		server.logger,
		queryIdentifier,
		queryText,
		databaseName,
		settings,
		server.configuration.defaultResultPreviewRows,
	)

	server.querySessionStore.create(session)

	writeJson(httpResponseWriter, http.StatusOK, map[string]any{
		"query_id":   queryIdentifier,
		"stream_url": fmt.Sprintf("/api/query/stream?query_id=%s", queryIdentifier),
	})
}

// buildDefaultQuerySettings returns ClickHouse settings applied to every query by default.
func buildDefaultQuerySettings(configuration applicationConfiguration) clickhouse.Settings {
	return clickhouse.Settings{
		// IMPORTANT: tu as dit que tu veux autoriser DELETE/DDL => pas de readonly.
		// "readonly": 1,

		// Guardrails
		"max_execution_time": configuration.defaultMaximumExecutionTimeSeconds,
		"max_result_rows":    configuration.defaultMaximumResultRows,
		"max_result_bytes":   configuration.defaultMaximumResultBytes,
		"send_logs_level":    "information",
	}
}

// cancelQueryRequest is the request payload for POST /api/query/cancel.
type cancelQueryRequest struct {
	QueryIdentifier string `json:"query_id"`
}

// handleCancelQuery handles POST /api/query/cancel.
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

	httpResponseWriter.Header().Set("Content-Type", "text/event-stream")
	httpResponseWriter.Header().Set("Cache-Control", "no-cache")
	httpResponseWriter.Header().Set("Connection", "keep-alive")
	httpResponseWriter.Header().Set("X-Accel-Buffering", "no")

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

	// Start query (async).
	session.start(clickhouseConnection, server.querySessionStore)

	// Cancel if client disconnects.
	go func() {
		<-httpRequest.Context().Done()
		session.requestCancellation()
	}()

	// Publish cadence (stable window for "instant" rates).
	publishTicker := time.NewTicker(250 * time.Millisecond)
	defer publishTicker.Stop()

	keepAliveTicker := time.NewTicker(15 * time.Second)
	defer keepAliveTicker.Stop()

	// ---- metrics state (we only compute "instant" + "max") ----
	var (
		prevSampleTime time.Time

		prevReadRows  uint64
		prevReadBytes uint64

		prevCPUSeconds float64

		cpuInstMaxPercent float64

		threadPeakMax int

		peakMemMaxBytes *int64
	)

	updateMaxPeakMem := func(v *int64) {
		if v == nil {
			return
		}
		if peakMemMaxBytes == nil || *v > *peakMemMaxBytes {
			copied := *v
			peakMemMaxBytes = &copied
		}
	}

	// Build "resource" payload from a snapshot.
	buildResourcePayload := func(now time.Time, snap querySessionSnapshot) map[string]any {
		// CPU seconds come from ClickHouse profile events (server-side), not API host.
		cpuSeconds := float64(snap.userTimeMicrosecondsTotal+snap.systemTimeMicrosecondsTotal) / 1e6

		// Estimate threads from "thread last seen" map.
		threadCountCurrent, threadCountPeak := estimateThreadCounts(now, snap.threadLastSeenByIdentifier, 2*time.Second, snap.threadPeakCount)
		if threadCountPeak > threadPeakMax {
			threadPeakMax = threadCountPeak
		}

		// Memory: best effort from profile events + logs parsing (peak).
		updateMaxPeakMem(snap.peakMemoryBytes)

		// Instant rates (stable window = publishTicker interval)
		rowsPerSecondInst := 0.0
		bytesPerSecondInst := 0.0
		cpuInstPercent := 0.0

		if !prevSampleTime.IsZero() {
			dt := now.Sub(prevSampleTime).Seconds()
			if dt > 0 {
				rowsPerSecondInst = float64(snap.readRowsTotal-prevReadRows) / dt
				bytesPerSecondInst = float64(snap.readBytesTotal-prevReadBytes) / dt

				cpuDelta := cpuSeconds - prevCPUSeconds
				if cpuDelta < 0 {
					cpuDelta = 0
				}
				cpuInstPercent = (cpuDelta / dt) * 100.0
				if cpuInstPercent > cpuInstMaxPercent {
					cpuInstMaxPercent = cpuInstPercent
				}
			}
		}

		prevSampleTime = now
		prevReadRows = snap.readRowsTotal
		prevReadBytes = snap.readBytesTotal
		prevCPUSeconds = cpuSeconds

		payload := map[string]any{
			"query_id": queryIdentifier,

			// Inst + Max only (simple UI)
			"rows_per_second_inst":  rowsPerSecondInst,
			"bytes_per_second_inst": bytesPerSecondInst,

			"cpu_percent_inst":     cpuInstPercent,
			"cpu_percent_inst_max": cpuInstMaxPercent,

			"thread_count_inst":     threadCountCurrent,
			"thread_count_inst_max": threadPeakMax,
		}

		if snap.currentMemoryBytes != nil {
			payload["memory_bytes_inst"] = *snap.currentMemoryBytes
		} else {
			payload["memory_bytes_inst"] = nil
		}

		if peakMemMaxBytes != nil {
			payload["memory_bytes_inst_max"] = *peakMemMaxBytes
		} else {
			payload["memory_bytes_inst_max"] = nil
		}

		return payload
	}

	// ---- progress payload ----
	buildProgressPayload := func(now time.Time, snap querySessionSnapshot) map[string]any {
		elapsedSeconds := 0.0
		if !snap.startedTime.IsZero() {
			elapsedSeconds = now.Sub(snap.startedTime).Seconds()
		}

		percent, known := calculateProgressPercent(
			0,
			snap.readBytesTotal,
			snap.totalRowsToRead,
			snap.readRowsTotal,
		)

		return map[string]any{
			"query_id":        queryIdentifier,
			"elapsed_seconds": elapsedSeconds,

			"percent":       percent,
			"percent_known": known,

			"read_rows":          snap.readRowsTotal,
			"read_bytes":         snap.readBytesTotal,
			"total_rows_to_read": snap.totalRowsToRead,
		}
	}

	// Results are already batched on the producer side (query_session.go) as "result_rows".
	// Here we just forward SSE events as-is.
	drainResultChannel := func() {
		for {
			select {
			case resultMessage := <-session.resultEventChannel:
				_ = writeServerSentEvent(httpResponseWriter, flusher, resultMessage.eventName, resultMessage.payload)
			default:
				return
			}
		}
	}

	// Initial publish.
	now := time.Now()
	snap := session.snapshot(now)
	_ = writeServerSentEvent(httpResponseWriter, flusher, "progress", buildProgressPayload(now, snap))
	_ = writeServerSentEvent(httpResponseWriter, flusher, "resource", buildResourcePayload(now, snap))

	for {
		select {
		case resultMessage := <-session.resultEventChannel:
			_ = writeServerSentEvent(httpResponseWriter, flusher, resultMessage.eventName, resultMessage.payload)

		case criticalMessage := <-session.criticalEventChannel:
			if criticalMessage.eventName == "done" {
				// Drain any buffered result events first.
				drainResultChannel()

				// Final metrics snapshot (after query end).
				finalNow := time.Now()
				finalSnap := session.snapshot(finalNow)

				_ = writeServerSentEvent(httpResponseWriter, flusher, "progress", buildProgressPayload(finalNow, finalSnap))
				_ = writeServerSentEvent(httpResponseWriter, flusher, "resource", buildResourcePayload(finalNow, finalSnap))

				// OPTIONAL: emit zeros so UI can reset immediately without logic.
				_ = writeServerSentEvent(httpResponseWriter, flusher, "resource", map[string]any{
					"query_id":              queryIdentifier,
					"rows_per_second_inst":  0,
					"bytes_per_second_inst": 0,
					"cpu_percent_inst":      0,
					"cpu_percent_inst_max":  cpuInstMaxPercent,
					"thread_count_inst":     0,
					"thread_count_inst_max": threadPeakMax,
					"memory_bytes_inst":     0,
					"memory_bytes_inst_max": func() any {
						if peakMemMaxBytes == nil {
							return nil
						}
						return *peakMemMaxBytes
					}(),
				})

				_ = writeServerSentEvent(httpResponseWriter, flusher, "done", criticalMessage.payload)
				return
			}

			_ = writeServerSentEvent(httpResponseWriter, flusher, criticalMessage.eventName, criticalMessage.payload)

		case <-publishTicker.C:
			t := time.Now()
			s := session.snapshot(t)

			_ = writeServerSentEvent(httpResponseWriter, flusher, "progress", buildProgressPayload(t, s))
			_ = writeServerSentEvent(httpResponseWriter, flusher, "resource", buildResourcePayload(t, s))

		case <-keepAliveTicker.C:
			_ = writeServerSentEvent(httpResponseWriter, flusher, "keepalive", map[string]any{
				"query_id": queryIdentifier,
				"time":     time.Now().Format(time.RFC3339),
			})

		case <-httpRequest.Context().Done():
			session.requestCancellation()
			return

		case <-session.nonCriticalEventChannel:
			// no-op
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
