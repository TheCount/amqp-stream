package stream

import (
	"net"
)

// tempError describes a temporary error.
type tempError struct {
	error
}

var _ net.Error = tempError{}

// Temporary implements net.Error.Temporary
func (te tempError) Temporary() bool {
	return true
}

// Timeout implements net.Error.Timeout.
func (te tempError) Timeout() bool {
	return false
}
