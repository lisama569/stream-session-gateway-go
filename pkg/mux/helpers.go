package mux

func (c *Conn) signalActivity() {
	select {
	case c.activity <- struct{}{}:
	default:
	}
}

func (c *Conn) signalClosing() {
	c.signalOnce.Do(func() { close(c.closeSignal) })
}

func (c *Conn) isForced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == StateForceTerminated
}

func (c *Conn) isClosing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == StateClosing
}
