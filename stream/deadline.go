package stream

import (
	"sync"
	"time"
)

// deadline implements a deadline useful for net.Conn implementers.
type deadline struct {
	// mx protects members of this structure from concurrent access.
	mx sync.Mutex

	// done is closed whenever this deadline exceeds.
	done signalChan

	// timer controls when done should be closed.
	// It is nil if there is currently no deadline,
	// or the deadline is in the past.
	timer *time.Timer
}

// Set sets this deadline. If t is the zero time, it will never expire.
func (dl *deadline) Set(t time.Time) {
	dl.mx.Lock()
	defer dl.mx.Unlock()

	if dl.timer != nil && !dl.timer.Stop() {
		<-dl.done // Stop does not wait for the AfterFunc to complete.
	}
	dl.timer = nil

	// No deadline
	if t.IsZero() {
		if dl.done.isClosed() {
			dl.done = make(signalChan)
		}
		return
	}

	// Future deadline
	if d := time.Until(t); d > 0 {
		if dl.done.isClosed() {
			dl.done = make(signalChan)
		}
		dl.timer = time.AfterFunc(d, func() {
			close(dl.done)
		})
		return
	}

	// Past deadline
	if !dl.done.isClosed() {
		close(dl.done)
	}
}

// Done returns a channel which is closed when this deadline expires.
func (dl *deadline) Done() signalChan {
	dl.mx.Lock()
	defer dl.mx.Unlock()

	return dl.done
}

// newDeadline returns a new deadline. The result must not be copied once
// in use.
func newDeadline() deadline {
	return deadline{
		done: make(signalChan),
	}
}
