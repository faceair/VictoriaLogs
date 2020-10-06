package lokipb

// Reset resets wr.
func (m *WriteRequest) Reset() {
	for i := range m.Streams {
		ts := &m.Streams[i]
		ts.Labels = ""
		ts.Entries = nil
	}
	m.Streams = m.Streams[:0]
}
