package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// querySessionStatus represents the lifecycle status of a query session.
type querySessionStatus string

const (
	querySessionStatusCreated            querySessionStatus = "created"
	querySessionStatusRunning            querySessionStatus = "running"
	querySessionStatusFinished           querySessionStatus = "finished"
	querySessionStatusErrored            querySessionStatus = "error"
	querySessionStatusCanceled           querySessionStatus = "canceled"
	querySessionStatusResultLimitReached querySessionStatus = "result_limit_reached"
)

// IMPORTANT: batching côté producteur (executeQuery)
// => évite de remplir resultEventChannel si le navigateur est plus lent que la DB.
const (
	resultBatchRows   = 200
	resultBatchPeriod = 50 * time.Millisecond
)

// Telemetry samples (pour les graph) : on collecte des points *plus fins* côté Go (callbacks CH)
// et on les pousse via SSE (batch) sans impacter le stream de résultats.
const (
	telemetryMinPointPeriod   = 80 * time.Millisecond   // throttle: max ~12.5 points/s
	telemetryFlushPeriod      = 350 * time.Millisecond  // flush batch au plus tard toutes les 350ms
	telemetryFlushMaxSamples  = 32                      // flush si on accumule trop
	telemetryMaxBufferedDrop  = 512                     // garde-fou mémoire (drop oldest)
)

// telemetrySample is a single timeseries point to feed front-end graphs.
type telemetrySample struct {
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	ReadRows            uint64  `json:"read_rows"`
	ReadBytes           uint64  `json:"read_bytes"`
	RowsPerSecondInst   float64 `json:"rows_per_second_inst"`
	BytesPerSecondInst  float64 `json:"bytes_per_second_inst"`
	CPUPercentInst      float64 `json:"cpu_percent_inst"`
	MemoryBytesInst     *int64  `json:"memory_bytes_inst"`
	ThreadCountInst     int     `json:"thread_count_inst"`
}

// querySession contains state and callbacks for a single query execution.
//
// The session is created by POST /api/query, started on first SSE attach (GET /api/query/stream),
// and can be canceled via POST /api/query/cancel or by closing the SSE connection.
type querySession struct {
	logger *slog.Logger

	queryIdentifier string
	queryText       string
	databaseName    string
	settings        clickhouse.Settings

	createdTime time.Time

	startOnce sync.Once
	mutex     sync.Mutex

	status querySessionStatus

	startedTime  time.Time
	finishedTime time.Time

	cancellationRequestedBeforeStart bool

	executionContext context.Context
	cancelExecution  context.CancelFunc

	nonCriticalEventChannel chan serverSentEventsMessage
	criticalEventChannel    chan serverSentEventsMessage
	resultEventChannel      chan serverSentEventsMessage

	readRowsTotal   uint64
	readBytesTotal  uint64
	totalRowsToRead uint64

	wroteRowsTotal  uint64
	wroteBytesTotal uint64

	userTimeMicrosecondsTotal   int64
	systemTimeMicrosecondsTotal int64

	currentMemoryBytes *int64
	peakMemoryBytes    *int64

	threadLastSeenByIdentifier map[uint64]time.Time
	threadPeakCount            int

	resultPreviewRowLimit int
	resultRowsReturned    int
	resultTruncated       bool

	// --- telemetry (graph) ---
	telemetrySamples        []telemetrySample
	telemetryLastPointTime  time.Time
	telemetryLastFlushTime  time.Time

	telemetryPrevTime       time.Time
	telemetryPrevReadRows   uint64
	telemetryPrevReadBytes  uint64
	telemetryPrevUserUs     int64
	telemetryPrevSystemUs   int64
}

// querySessionSnapshot is an immutable view of a session at a specific time.
type querySessionSnapshot struct {
	queryIdentifier string
	status          querySessionStatus

	createdTime  time.Time
	startedTime  time.Time
	finishedTime time.Time

	readRowsTotal   uint64
	readBytesTotal  uint64
	totalRowsToRead uint64

	wroteRowsTotal  uint64
	wroteBytesTotal uint64

	userTimeMicrosecondsTotal   int64
	systemTimeMicrosecondsTotal int64

	currentMemoryBytes *int64
	peakMemoryBytes    *int64

	threadLastSeenByIdentifier map[uint64]time.Time
	threadPeakCount            int
}

// newQuerySession creates a new query session.
func newQuerySession(
	logger *slog.Logger,
	queryIdentifier string,
	queryText string,
	databaseName string,
	settings clickhouse.Settings,
	resultPreviewRowLimit int,
) *querySession {
	return &querySession{
		logger: logger,

		queryIdentifier: queryIdentifier,
		queryText:       queryText,
		databaseName:    databaseName,
		settings:        settings,

		createdTime: time.Now(),
		status:      querySessionStatusCreated,

		nonCriticalEventChannel: make(chan serverSentEventsMessage, 256),
		criticalEventChannel:    make(chan serverSentEventsMessage, 64),
		resultEventChannel:      make(chan serverSentEventsMessage, 2048),

		threadLastSeenByIdentifier: make(map[uint64]time.Time),

		resultPreviewRowLimit: resultPreviewRowLimit,

		telemetrySamples: make([]telemetrySample, 0, telemetryFlushMaxSamples),
	}
}

// trySendResult sends result rows; if the client is too slow, we cancel to avoid OOM.
func (session *querySession) trySendResult(message serverSentEventsMessage) {
	select {
	case session.resultEventChannel <- message:
		return
	default:
		// The client is too slow: do not accumulate unbounded memory.
		session.requestCancellation()
		session.trySendCritical(serverSentEventsMessage{
			eventName: "error",
			payload: map[string]any{
				"query_id": session.queryIdentifier,
				"message":  "client is too slow to consume result stream",
			},
		})
	}
}

// trySendTelemetry is best-effort: drops when client is slow.
// We keep telemetry on nonCritical channel so it doesn't interfere with result rows or "done".
func (session *querySession) trySendTelemetry(message serverSentEventsMessage) {
	select {
	case session.nonCriticalEventChannel <- message:
	default:
	}
}

func (session *querySession) start(clickhouseConnection clickhouseDriver.Conn, sessionStore *querySessionStore) {
	session.startOnce.Do(func() {
		session.mutex.Lock()
		defer session.mutex.Unlock()

		if session.cancellationRequestedBeforeStart {
			// Nothing to execute; the cancel endpoint was called before the stream attached.
			session.trySendCritical(serverSentEventsMessage{
				eventName: "done",
				payload:   session.donePayload(time.Now(), querySessionStatusCanceled, nil),
			})
			return
		}

		session.executionContext, session.cancelExecution = context.WithCancel(context.Background())
		session.startedTime = time.Now()
		session.status = querySessionStatusRunning

		// init telemetry baseline
		session.telemetryPrevTime = time.Time{}
		session.telemetryLastPointTime = time.Time{}
		session.telemetryLastFlushTime = session.startedTime

		session.logger.Info("query started",
			"query_identifier", session.queryIdentifier,
			"database_name", session.databaseName,
			"query_preview", truncateForLogs(session.queryText, 140),
		)

		go session.executeQuery(clickhouseConnection, sessionStore)
	})
}

// requestCancellation requests cancellation of the query session (best effort).
func (session *querySession) requestCancellation() {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if session.status == querySessionStatusFinished || session.status == querySessionStatusErrored || session.status == querySessionStatusCanceled {
		return
	}

	if session.cancelExecution != nil {
		session.status = querySessionStatusCanceled
		session.cancelExecution()
		return
	}

	// The query has not started yet; remember cancellation and mark as canceled.
	session.cancellationRequestedBeforeStart = true
	session.status = querySessionStatusCanceled
	session.finishedTime = time.Now()
}

// executeQuery runs the ClickHouse query and drains the result stream.
// It sends final "done" events (and "error" when applicable) through the critical event channel.
func (session *querySession) executeQuery(clickhouseConnection clickhouseDriver.Conn, sessionStore *querySessionStore) {
	executionStartedTime := time.Now()
	defer func() {
		sessionStore.remove(session.queryIdentifier)
	}()

	baseCtx := session.executionContext

	// IMPORTANT: attach progress/profile/logs via clickhouse.Context
	ctx := clickhouse.Context(
		baseCtx,
		clickhouse.WithSettings(session.settings),
		clickhouse.WithProgress(session.onProgress),
		clickhouse.WithProfileInfo(session.onProfileInfo),
		clickhouse.WithProfileEvents(session.onProfileEvents),
		clickhouse.WithLogs(session.onLog),
	)

	rows, queryError := clickhouseConnection.Query(ctx, session.queryText)
	if queryError != nil {
		session.finishWithError(time.Now(), queryError, executionStartedTime)
		return
	}
	defer rows.Close()

	columnNames := rows.Columns()
	columnTypeNames, typeError := resolveDatabaseTypeNames(rows)

	if typeError != nil || len(columnTypeNames) != len(columnNames) {
		// If we cannot resolve types, we should not “guess string” because it will crash on numbers.
		// Fail early with an actionable error.
		session.finishWithError(time.Now(), fmt.Errorf("cannot resolve result column types: %w", typeError), executionStartedTime)
		return
	}

	session.trySendCritical(serverSentEventsMessage{
		eventName: "result_meta",
		payload: map[string]any{
			"query_id": session.queryIdentifier,
			"columns":  columnNames,
			"types":    columnTypeNames,
		},
	})

	columnCount := len(columnNames)

	// Allocate correct scan destinations once and reuse them for every row.
	scanDestinations := make([]any, columnCount) // pointers given to rows.Scan(...)
	valuePointers := make([]any, columnCount)    // same pointers, used for stringify
	for columnIndex := 0; columnIndex < columnCount; columnIndex++ {
		destinationPointer := allocateScanPointerForDatabaseType(columnTypeNames[columnIndex])
		scanDestinations[columnIndex] = destinationPointer
		valuePointers[columnIndex] = destinationPointer
	}

	// Batch result rows on the producer side to avoid filling session.resultEventChannel.
	batch := make([][]string, 0, resultBatchRows)
	lastFlush := time.Now()

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}

		session.trySendResult(serverSentEventsMessage{
			eventName: "result_rows",
			payload: map[string]any{
				"query_id": session.queryIdentifier,
				"rows":     batch,
			},
		})

		batch = make([][]string, 0, resultBatchRows)
		lastFlush = time.Now()

		// If trySendResult detected a slow client, it requests cancellation.
		session.mutex.Lock()
		canceled := session.status == querySessionStatusCanceled
		session.mutex.Unlock()
		return !canceled
	}

	for rows.Next() {
		if scanError := rows.Scan(scanDestinations...); scanError != nil {
			session.finishWithError(time.Now(), scanError, executionStartedTime)
			return
		}

		formattedRow := make([]string, columnCount)
		for columnIndex := 0; columnIndex < columnCount; columnIndex++ {
			formattedRow[columnIndex] = stringifyScanPointer(valuePointers[columnIndex])
		}

		session.mutex.Lock()
		session.resultRowsReturned++
		reachedLimit := session.resultPreviewRowLimit > 0 && session.resultRowsReturned >= session.resultPreviewRowLimit
		if reachedLimit {
			session.resultTruncated = true
		}
		session.mutex.Unlock()

		batch = append(batch, formattedRow)

		// Flush: by batch size OR by time window (keeps UI reactive).
		if len(batch) >= resultBatchRows || time.Since(lastFlush) >= resultBatchPeriod {
			if ok := flush(); !ok {
				break
			}
		}

		if reachedLimit {
			_ = flush()
			session.requestCancellation()
			break
		}
	}

	_ = flush()

	if rowsError := rows.Err(); rowsError != nil {
		session.finishWithError(time.Now(), rowsError, executionStartedTime)
		return
	}

	session.finishSuccessfully(time.Now(), executionStartedTime)
}

// finishSuccessfully marks the session as finished and sends the final done event.
func (session *querySession) finishSuccessfully(finishedTime time.Time, executionStartedTime time.Time) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	// push a last telemetry point + flush before "done"
	session.recordTelemetrySampleLocked(finishedTime)
	session.flushTelemetryLocked(finishedTime)

	// If the query was canceled because the result preview limit was reached,
	// report a distinct final status.
	if session.status == querySessionStatusCanceled && session.resultTruncated {
		session.finishedTime = finishedTime
		session.trySendCritical(serverSentEventsMessage{
			eventName: "done",
			payload:   session.donePayload(finishedTime, querySessionStatusResultLimitReached, nil),
		})
		session.logger.Info("query stopped because result limit was reached",
			"query_identifier", session.queryIdentifier,
			"duration", finishedTime.Sub(executionStartedTime),
			"result_rows_returned", session.resultRowsReturned,
		)
		return
	}

	if session.status == querySessionStatusCanceled {
		session.finishedTime = finishedTime
		session.trySendCritical(serverSentEventsMessage{
			eventName: "done",
			payload:   session.donePayload(finishedTime, querySessionStatusCanceled, nil),
		})
		session.logger.Info("query canceled (completed after cancellation)",
			"query_identifier", session.queryIdentifier,
			"duration", finishedTime.Sub(executionStartedTime),
		)
		return
	}

	session.status = querySessionStatusFinished
	session.finishedTime = finishedTime

	session.trySendCritical(serverSentEventsMessage{
		eventName: "done",
		payload:   session.donePayload(finishedTime, querySessionStatusFinished, nil),
	})

	session.logger.Info("query finished",
		"query_identifier", session.queryIdentifier,
		"duration", finishedTime.Sub(executionStartedTime),
		"read_rows_total", session.readRowsTotal,
		"read_bytes_total", session.readBytesTotal,
		"result_rows_returned", session.resultRowsReturned,
	)
}

// finishWithError marks the session as errored or canceled and sends "error" (when appropriate) + "done".
func (session *querySession) finishWithError(finishedTime time.Time, executionError error, executionStartedTime time.Time) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	// push a last telemetry point + flush before "done"
	session.recordTelemetrySampleLocked(finishedTime)
	session.flushTelemetryLocked(finishedTime)

	isCancellation := errors.Is(executionError, context.Canceled)

	if session.status == querySessionStatusCanceled || isCancellation {
		finalStatus := querySessionStatusCanceled
		if session.resultTruncated {
			finalStatus = querySessionStatusResultLimitReached
		}

		session.status = querySessionStatusCanceled
		session.finishedTime = finishedTime

		session.trySendCritical(serverSentEventsMessage{
			eventName: "done",
			payload:   session.donePayload(finishedTime, finalStatus, executionError),
		})

		session.logger.Info("query canceled",
			"query_identifier", session.queryIdentifier,
			"duration", finishedTime.Sub(executionStartedTime),
		)
		return
	}

	session.status = querySessionStatusErrored
	session.finishedTime = finishedTime

	session.trySendCritical(serverSentEventsMessage{
		eventName: "error",
		payload:   buildErrorPayload(session.queryIdentifier, executionError),
	})

	session.trySendCritical(serverSentEventsMessage{
		eventName: "done",
		payload:   session.donePayload(finishedTime, querySessionStatusErrored, executionError),
	})

	session.logger.Info("query errored",
		"query_identifier", session.queryIdentifier,
		"duration", finishedTime.Sub(executionStartedTime),
		"error", executionError.Error(),
	)
}

// onProfileInfo is a ClickHouse callback invoked with per-query profile info (summary).
func (session *querySession) onProfileInfo(profileInfo *clickhouse.ProfileInfo) {
	now := time.Now()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	// ProfileInfo = valeurs globales (souvent envoyées à la fin).
	// On garde le max au cas où on reçoit plusieurs fois.
	if profileInfo.Rows > session.readRowsTotal {
		session.readRowsTotal = profileInfo.Rows
	}
	if profileInfo.Bytes > session.readBytesTotal {
		session.readBytesTotal = profileInfo.Bytes
	}

	session.recordTelemetrySampleLocked(now)
}

// onProgress is a ClickHouse callback invoked with query progress deltas.
func (session *querySession) onProgress(progress *clickhouse.Progress) {
	now := time.Now()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.readRowsTotal += progress.Rows
	session.readBytesTotal += progress.Bytes
	session.wroteRowsTotal += progress.WroteRows
	session.wroteBytesTotal += progress.WroteBytes

	if progress.TotalRows > session.totalRowsToRead {
		session.totalRowsToRead = progress.TotalRows
	}

	session.recordTelemetrySampleLocked(now)
}

// onProfileEvents is a ClickHouse callback invoked with per-query profile events.
func (session *querySession) onProfileEvents(profileEvents []clickhouse.ProfileEvent) {
	now := time.Now()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	for _, e := range profileEvents {
		session.threadLastSeenByIdentifier[e.ThreadID] = now

		// CPU
		switch e.Name {
		case "UserTimeMicroseconds", "OSUserTimeMicroseconds":
			session.userTimeMicrosecondsTotal += e.Value
		case "SystemTimeMicroseconds", "OSSystemTimeMicroseconds":
			session.systemTimeMicrosecondsTotal += e.Value
		}

		// ---- MEMORY (robuste) ----
		// Certaines versions/builds envoient des noms différents.
		// On capture tout ce qui contient "Memory" et on met à jour inst/peak intelligemment.
		if strings.Contains(e.Name, "Memory") || strings.Contains(e.Name, "Mem") {
			v := int64(e.Value)

			// "current" / inst : MemoryTracking, MemoryUsage, CurrentMemoryUsage, etc.
			if strings.Contains(e.Name, "Tracking") ||
				strings.Contains(e.Name, "Current") ||
				strings.Contains(e.Name, "Usage") {
				session.currentMemoryBytes = &v
			}

			// "peak"
			if strings.Contains(e.Name, "Peak") {
				if session.peakMemoryBytes == nil || v > *session.peakMemoryBytes {
					session.peakMemoryBytes = &v
				}
			}
		}
	}

	session.recordTelemetrySampleLocked(now)
}

// onLog is a ClickHouse callback invoked with server log messages.
func (session *querySession) onLog(logEntry *clickhouse.Log) {
	// The UI does not display log lines.
	// We only keep this callback to best-effort extract peak memory usage from server logs.
	session.mutex.Lock()
	defer session.mutex.Unlock()

	parsedPeakBytes, parsed := parsePeakMemoryUsageFromLogLine(logEntry.Text)
	if parsed {
		valueAsBytes := int64(parsedPeakBytes)
		session.peakMemoryBytes = &valueAsBytes
	}
}

// appendLogLine adds a log line to a fixed-size ring buffer and emits a best-effort SSE log event.
func (session *querySession) appendLogLine(line string) {
	// Intentionally disabled: the UI does not display logs.
	_ = line
}

// snapshot returns a copy of the current session state.
func (session *querySession) snapshot(now time.Time) querySessionSnapshot {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	threadLastSeenCopy := make(map[uint64]time.Time, len(session.threadLastSeenByIdentifier))
	for threadIdentifier, lastSeen := range session.threadLastSeenByIdentifier {
		threadLastSeenCopy[threadIdentifier] = lastSeen
	}

	return querySessionSnapshot{
		queryIdentifier: session.queryIdentifier,
		status:          session.status,

		createdTime:  session.createdTime,
		startedTime:  session.startedTime,
		finishedTime: session.finishedTime,

		readRowsTotal:   session.readRowsTotal,
		readBytesTotal:  session.readBytesTotal,
		totalRowsToRead: session.totalRowsToRead,

		wroteRowsTotal:  session.wroteRowsTotal,
		wroteBytesTotal: session.wroteBytesTotal,

		userTimeMicrosecondsTotal:   session.userTimeMicrosecondsTotal,
		systemTimeMicrosecondsTotal: session.systemTimeMicrosecondsTotal,

		currentMemoryBytes: session.currentMemoryBytes,
		peakMemoryBytes:    session.peakMemoryBytes,

		threadLastSeenByIdentifier: threadLastSeenCopy,
		threadPeakCount:            session.threadPeakCount,
	}
}

// donePayload builds a "done" event payload.
func (session *querySession) donePayload(now time.Time, status querySessionStatus, executionError error) map[string]any {
	elapsedSeconds := 0.0
	if !session.startedTime.IsZero() {
		elapsedSeconds = now.Sub(session.startedTime).Seconds()
	}

	payload := map[string]any{
		"query_id":        session.queryIdentifier,
		"status":          status,
		"elapsed_seconds": elapsedSeconds,
		"read_rows":       session.readRowsTotal,
		"read_bytes":      session.readBytesTotal,

		"result_rows_returned": session.resultRowsReturned,
		"result_truncated":     session.resultTruncated,
	}

	if executionError != nil {
		payload["message"] = executionError.Error()
	}

	return payload
}

// recordTelemetrySampleLocked appends a point for graphs (best-effort throttled).
// IMPORTANT: must be called with session.mutex held.
func (session *querySession) recordTelemetrySampleLocked(now time.Time) {
	if session.startedTime.IsZero() {
		return
	}

	// throttle point creation
	if !session.telemetryLastPointTime.IsZero() && now.Sub(session.telemetryLastPointTime) < telemetryMinPointPeriod {
		// still flush if needed (rare)
		session.flushTelemetryLocked(now)
		return
	}
	session.telemetryLastPointTime = now

	elapsedSeconds := now.Sub(session.startedTime).Seconds()

	// thread count estimate (based on last seen in callbacks)
	threadCountInst, threadPeak := estimateThreadCounts(
		now,
		session.threadLastSeenByIdentifier,
		2*time.Second,
		session.threadPeakCount,
	)
	session.threadPeakCount = threadPeak

	// instantaneous deltas
	var rowsPerSec, bytesPerSec, cpuPct float64

	if !session.telemetryPrevTime.IsZero() {
		dt := now.Sub(session.telemetryPrevTime).Seconds()
		if dt > 0 {
			rowsDelta := session.readRowsTotal - session.telemetryPrevReadRows
			bytesDelta := session.readBytesTotal - session.telemetryPrevReadBytes

			rowsPerSec = float64(rowsDelta) / dt
			bytesPerSec = float64(bytesDelta) / dt

			cpuDeltaUs := (session.userTimeMicrosecondsTotal - session.telemetryPrevUserUs) +
				(session.systemTimeMicrosecondsTotal - session.telemetryPrevSystemUs)

			cpuPct = (float64(cpuDeltaUs) / 1_000_000.0) / dt * 100.0
		}
	}

	// update baseline for next point
	session.telemetryPrevTime = now
	session.telemetryPrevReadRows = session.readRowsTotal
	session.telemetryPrevReadBytes = session.readBytesTotal
	session.telemetryPrevUserUs = session.userTimeMicrosecondsTotal
	session.telemetryPrevSystemUs = session.systemTimeMicrosecondsTotal

	session.telemetrySamples = append(session.telemetrySamples, telemetrySample{
		ElapsedSeconds:     elapsedSeconds,
		ReadRows:           session.readRowsTotal,
		ReadBytes:          session.readBytesTotal,
		RowsPerSecondInst:  rowsPerSec,
		BytesPerSecondInst: bytesPerSec,
		CPUPercentInst:     cpuPct,
		MemoryBytesInst:    session.currentMemoryBytes,
		ThreadCountInst:    threadCountInst,
	})

	// garde-fou mémoire
	if len(session.telemetrySamples) > telemetryMaxBufferedDrop {
		session.telemetrySamples = session.telemetrySamples[len(session.telemetrySamples)-telemetryMaxBufferedDrop:]
	}

	session.flushTelemetryLocked(now)
}

// flushTelemetryLocked pushes a batch if time/size threshold reached.
// IMPORTANT: must be called with session.mutex held.
func (session *querySession) flushTelemetryLocked(now time.Time) {
	if len(session.telemetrySamples) == 0 {
		return
	}

	needFlush := false
	if len(session.telemetrySamples) >= telemetryFlushMaxSamples {
		needFlush = true
	} else if session.telemetryLastFlushTime.IsZero() || now.Sub(session.telemetryLastFlushTime) >= telemetryFlushPeriod {
		needFlush = true
	}

	if !needFlush {
		return
	}

	// detach slice for send
	samples := session.telemetrySamples
	session.telemetrySamples = make([]telemetrySample, 0, telemetryFlushMaxSamples)
	session.telemetryLastFlushTime = now

	session.trySendTelemetry(serverSentEventsMessage{
		eventName: "samples",
		payload: map[string]any{
			"query_id": session.queryIdentifier,
			"samples":  samples,
		},
	})
}

// trySendNonCritical sends a message without blocking; if the client is slow, the message is dropped.
func (session *querySession) trySendNonCritical(message serverSentEventsMessage) {
	select {
	case session.nonCriticalEventChannel <- message:
	default:
	}
}

// trySendCritical sends a message without blocking; if the client is slow or disconnected, the message is dropped.
func (session *querySession) trySendCritical(message serverSentEventsMessage) {
	select {
	case session.criticalEventChannel <- message:
	default:
	}
}

func endsWithFormatClause(queryText string) bool {
	trimmed := strings.TrimSpace(queryText)
	if trimmed == "" {
		return false
	}

	// Remove trailing semicolons/spaces.
	for strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}

	lower := strings.ToLower(trimmed)

	// We only accept "format <name>" if it is at the very end of the query
	// (no extra tokens after the format name).
	//
	// This avoids false positives where "format" appears inside strings, comments,
	// identifiers, or subqueries.
	lastFormatIndex := strings.LastIndex(lower, " format ")
	if lastFormatIndex < 0 {
		return false
	}

	after := strings.TrimSpace(lower[lastFormatIndex+len(" format "):])
	if after == "" {
		return false
	}

	// Format name is expected to be a single token.
	// If there are more tokens after it, we do not treat it as a terminal FORMAT clause.
	parts := strings.Fields(after)
	if len(parts) != 1 {
		return false
	}

	return true
}

func splitTabSeparatedLine(line string) []string {
	// TabSeparatedWithNamesAndTypes uses tab as delimiter.
	// Values are not quoted; special chars are escaped by ClickHouse rules.
	// For MVP we split on '\t' and keep raw strings.
	if line == "" {
		return []string{""}
	}
	return strings.Split(line, "\t")
}
