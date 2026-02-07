package main

import (
	"log/slog"
	"sync"
	"time"
)

// querySessionStore is an in-memory store of query sessions keyed by query identifier.
type querySessionStore struct {
	logger        *slog.Logger
	configuration applicationConfiguration

	mutex    sync.Mutex
	sessions map[string]*querySession

	now func() time.Time
}

// newQuerySessionStore creates a new query session store and starts a background cleanup loop.
func newQuerySessionStore(logger *slog.Logger, configuration applicationConfiguration) *querySessionStore {
	store := &querySessionStore{
		logger:        logger,
		configuration: configuration,
		sessions:      make(map[string]*querySession),
		now:           time.Now,
	}
	go store.cleanupLoop()
	return store
}

// create stores a new query session.
func (store *querySessionStore) create(session *querySession) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.sessions[session.queryIdentifier] = session
}

// get returns a query session by identifier.
func (store *querySessionStore) get(queryIdentifier string) (*querySession, bool) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	session, exists := store.sessions[queryIdentifier]
	return session, exists
}

// remove deletes a query session.
func (store *querySessionStore) remove(queryIdentifier string) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	delete(store.sessions, queryIdentifier)
}

// cleanupLoop periodically removes expired sessions to avoid unbounded memory growth.
func (store *querySessionStore) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := store.now()

		store.mutex.Lock()
		for queryIdentifier, session := range store.sessions {
			sessionSnapshot := session.snapshot(now)

			if sessionSnapshot.status == querySessionStatusCreated {
				if now.Sub(sessionSnapshot.createdTime) > store.configuration.sessionExpirationIfNotStarted {
					store.logger.Info("removing expired not-started query session", "query_identifier", queryIdentifier)
					delete(store.sessions, queryIdentifier)
				}
				continue
			}

			if sessionSnapshot.status == querySessionStatusFinished || sessionSnapshot.status == querySessionStatusErrored || sessionSnapshot.status == querySessionStatusCanceled {
				if !sessionSnapshot.finishedTime.IsZero() && now.Sub(sessionSnapshot.finishedTime) > store.configuration.sessionExpirationAfterFinish {
					store.logger.Info("removing expired finished query session", "query_identifier", queryIdentifier, "status", sessionSnapshot.status)
					delete(store.sessions, queryIdentifier)
				}
			}
		}
		store.mutex.Unlock()
	}
}
