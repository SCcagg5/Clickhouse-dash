package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// querySessionStatus represents the lifecycle status of a query session.
type querySessionStatus string

const (
	querySessionStatusCreated  querySessionStatus = "created"
	querySessionStatusRunning  querySessionStatus = "running"
	querySessionStatusFinished querySessionStatus = "finished"
	querySessionStatusErrored  querySessionStatus = "error"
	querySessionStatusCanceled querySessionStatus = "canceled"
)

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

	// Progress counters (server reports progress in deltas; these fields are accumulated totals).
	readRowsTotal   uint64
	readBytesTotal  uint64
	totalRowsToRead uint64

	wroteRowsTotal  uint64
	wroteBytesTotal uint64

	// Profile events totals.
	userTimeMicrosecondsTotal   int64
	systemTimeMicrosecondsTotal int64

	// Optional: memory tracking (best effort).
	currentMemoryBytes *int64
	peakMemoryBytes    *int64

	// Thread tracking (best effort).
	threadLastSeenByIdentifier map[uint64]time.Time
	threadPeakCount            int

	// Server logs (for UI log panel).
	logLinesRingBuffer           []string
	logLinesRingBufferNextIndex  int
	logLinesRingBufferCountValid int
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

	logLines []string
}

// newQuerySession creates a new query session.
func newQuerySession(
	logger *slog.Logger,
	queryIdentifier string,
	queryText string,
	databaseName string,
	settings clickhouse.Settings,
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
		criticalEventChannel:    make(chan serverSentEventsMessage, 16),

		threadLastSeenByIdentifier: make(map[uint64]time.Time),

		logLinesRingBuffer: make([]string, 200),
	}
}

// start begins query execution exactly once.
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
		// Removing the session is safe even if the SSE handler is still streaming: it holds its own pointer.
		sessionStore.remove(session.queryIdentifier)
	}()

	queryContext := clickhouse.Context(
		session.executionContext,
		clickhouse.WithQueryID(session.queryIdentifier),
		clickhouse.WithSettings(session.settings),
		clickhouse.WithProgress(session.onProgress),
		clickhouse.WithProfileEvents(session.onProfileEvents),
		clickhouse.WithLogs(session.onLog),
	)

	rows, queryError := clickhouseConnection.Query(queryContext, session.queryText)
	if queryError != nil {
		session.finishWithError(time.Now(), queryError, executionStartedTime)
		return
	}

	// Drain rows without materializing them. The query remains "in flight" until the stream is consumed.
	for rows.Next() {
	}

	rowsError := rows.Err()
	closeError := rows.Close()

	if rowsError != nil {
		session.finishWithError(time.Now(), rowsError, executionStartedTime)
		return
	}
	if closeError != nil {
		session.finishWithError(time.Now(), closeError, executionStartedTime)
		return
	}

	session.finishSuccessfully(time.Now(), executionStartedTime)
}

// finishSuccessfully marks the session as finished and sends the final done event.
func (session *querySession) finishSuccessfully(finishedTime time.Time, executionStartedTime time.Time) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

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
	)
}

// finishWithError marks the session as errored or canceled and sends "error" (when appropriate) + "done".
func (session *querySession) finishWithError(finishedTime time.Time, executionError error, executionStartedTime time.Time) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	isCancellation := errors.Is(executionError, context.Canceled)

	if session.status == querySessionStatusCanceled || isCancellation {
		session.status = querySessionStatusCanceled
		session.finishedTime = finishedTime
		session.trySendCritical(serverSentEventsMessage{
			eventName: "done",
			payload:   session.donePayload(finishedTime, querySessionStatusCanceled, executionError),
		})
		session.logger.Info("query canceled",
			"query_identifier", session.queryIdentifier,
			"duration", finishedTime.Sub(executionStartedTime),
			"error", executionError.Error(),
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

	session.logger.Error("query errored",
		"query_identifier", session.queryIdentifier,
		"duration", finishedTime.Sub(executionStartedTime),
		"error", executionError.Error(),
	)
}

// onProgress is a ClickHouse callback invoked with query progress deltas.
func (session *querySession) onProgress(progress *clickhouse.Progress) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.readRowsTotal += progress.Rows
	session.readBytesTotal += progress.Bytes
	session.wroteRowsTotal += progress.WroteRows
	session.wroteBytesTotal += progress.WroteBytes

	if progress.TotalRows > session.totalRowsToRead {
		session.totalRowsToRead = progress.TotalRows
	}
}

// onProfileEvents is a ClickHouse callback invoked with per-query profile events.
func (session *querySession) onProfileEvents(profileEvents []clickhouse.ProfileEvent) {
	now := time.Now()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	for _, profileEvent := range profileEvents {
		session.threadLastSeenByIdentifier[profileEvent.ThreadID] = now

		if profileEvent.Name == "UserTimeMicroseconds" || profileEvent.Name == "OSUserTimeMicroseconds" {
			session.userTimeMicrosecondsTotal += profileEvent.Value
		}
		if profileEvent.Name == "SystemTimeMicroseconds" || profileEvent.Name == "OSSystemTimeMicroseconds" {
			session.systemTimeMicrosecondsTotal += profileEvent.Value
		}

		// Best-effort memory gauges (availability depends on server/version/settings).
		if profileEvent.Name == "MemoryTracking" || profileEvent.Name == "CurrentMemoryUsage" || profileEvent.Name == "MemoryUsage" {
			valueAsBytes := profileEvent.Value
			session.currentMemoryBytes = &valueAsBytes
		}
		if profileEvent.Name == "PeakMemoryUsage" {
			valueAsBytes := profileEvent.Value
			session.peakMemoryBytes = &valueAsBytes
		}
	}
}

// onLog is a ClickHouse callback invoked with server log messages.
func (session *querySession) onLog(logEntry *clickhouse.Log) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	logLine := fmt.Sprintf("%s [%s] %s", logEntry.Time.Format(time.RFC3339), logEntry.Source, logEntry.Text)
	session.appendLogLine(logLine)

	parsedPeakBytes, parsed := parsePeakMemoryUsageFromLogLine(logEntry.Text)
	if parsed {
		valueAsBytes := int64(parsedPeakBytes)
		session.peakMemoryBytes = &valueAsBytes
	}
}

// appendLogLine adds a log line to a fixed-size ring buffer and emits a best-effort SSE log event.
func (session *querySession) appendLogLine(line string) {
	if len(session.logLinesRingBuffer) == 0 {
		return
	}

	session.logLinesRingBuffer[session.logLinesRingBufferNextIndex] = line
	session.logLinesRingBufferNextIndex = (session.logLinesRingBufferNextIndex + 1) % len(session.logLinesRingBuffer)

	if session.logLinesRingBufferCountValid < len(session.logLinesRingBuffer) {
		session.logLinesRingBufferCountValid++
	}

	session.trySendNonCritical(serverSentEventsMessage{
		eventName: "log",
		payload: map[string]any{
			"query_id": session.queryIdentifier,
			"line":     line,
		},
	})
}

// snapshot returns a copy of the current session state.
func (session *querySession) snapshot(now time.Time) querySessionSnapshot {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	threadLastSeenCopy := make(map[uint64]time.Time, len(session.threadLastSeenByIdentifier))
	for threadIdentifier, lastSeen := range session.threadLastSeenByIdentifier {
		threadLastSeenCopy[threadIdentifier] = lastSeen
	}

	logLines := make([]string, 0, session.logLinesRingBufferCountValid)
	if session.logLinesRingBufferCountValid > 0 {
		startIndex := session.logLinesRingBufferNextIndex - session.logLinesRingBufferCountValid
		if startIndex < 0 {
			startIndex += len(session.logLinesRingBuffer)
		}
		for indexOffset := 0; indexOffset < session.logLinesRingBufferCountValid; indexOffset++ {
			logLines = append(logLines, session.logLinesRingBuffer[(startIndex+indexOffset)%len(session.logLinesRingBuffer)])
		}
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

		logLines: logLines,
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
	}
	if executionError != nil {
		payload["message"] = executionError.Error()
	}
	return payload
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
