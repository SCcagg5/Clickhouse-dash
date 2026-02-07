package main

// serverSentEventsMessage represents a single Server-Sent Events message to send to the browser.
type serverSentEventsMessage struct {
	eventName string
	payload   any
}
