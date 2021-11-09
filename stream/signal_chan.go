package stream

// signalChan is a signalling channel,
// i. e., a channel that is never written to, and closed only once to signal
// some condition that other goroutines are waiting for.
type signalChan chan struct{}

// isClosed non-destructively checks whether this channel is closed or not
// (non-destructiveness requires that nothing ever sends on this channel).
func (sc signalChan) isClosed() bool {
	select {
	case <-sc:
		return true
	default:
		return false
	}
}
