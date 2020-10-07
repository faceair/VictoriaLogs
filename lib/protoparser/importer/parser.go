package importer

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson/fastfloat"
)

// Rows contains parsed Prometheus rows.
type Rows struct {
	Rows []Row

	labelsPool []storage.Label
}

// Reset resets rs.
func (rs *Rows) Reset() {
	// Reset items, so they can be GC'ed

	for i := range rs.Rows {
		rs.Rows[i].reset()
	}
	rs.Rows = rs.Rows[:0]

	for i := range rs.labelsPool {
		label := rs.labelsPool[i]
		label.Name = nil
		label.Value = nil
	}
	rs.labelsPool = rs.labelsPool[:0]
}

// Unmarshal unmarshals Prometheus exposition text rows from s.
//
// See https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md#text-format-details
//
// s shouldn't be modified while rs is in use.
func (rs *Rows) Unmarshal(s []byte) {
	rs.UnmarshalWithErrLogger(s, stdErrLogger)
}

func stdErrLogger(s string) {
	logger.ErrorfSkipframes(1, "%s", s)
}

// UnmarshalWithErrLogger unmarshal Prometheus exposition text rows from s.
//
// It calls errLogger for logging parsing errors.
//
// s shouldn't be modified while rs is in use.
func (rs *Rows) UnmarshalWithErrLogger(s []byte, errLogger func(s string)) {
	noEscapes := bytes.IndexByte(s, '\\') < 0
	rs.Rows, rs.labelsPool = unmarshalRows(rs.Rows[:0], s, rs.labelsPool[:0], noEscapes, errLogger)
}

// Row is a single Prometheus row.
type Row struct {
	Metric    []byte
	Labels    []storage.Label
	Value     []byte
	Timestamp int64
}

func (r *Row) reset() {
	r.Metric = nil
	r.Labels = nil
	r.Value = nil
	r.Timestamp = 0
}

func skipLeadingWhitespace(s []byte) []byte {
	// Prometheus treats ' ' and '\t' as whitespace
	// according to https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md#text-format-details
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}

func skipTrailingWhitespace(s []byte) []byte {
	// Prometheus treats ' ' and '\t' as whitespace
	// according to https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md#text-format-details
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func nextWhitespace(s []byte) int {
	n := bytes.IndexByte(s, ' ')
	if n < 0 {
		return bytes.IndexByte(s, '\t')
	}
	n1 := bytes.IndexByte(s, '\t')
	if n1 < 0 || n1 > n {
		return n
	}
	return n1
}

func (r *Row) unmarshal(s []byte, labelsPool []storage.Label, noEscapes bool) ([]storage.Label, error) {
	r.reset()
	s = skipLeadingWhitespace(s)
	n := bytes.IndexByte(s, '{')
	if n >= 0 {
		// Labels found. Parse them.
		r.Metric = skipTrailingWhitespace(s[:n])
		s = s[n+1:]
		labelsStart := len(labelsPool)
		var err error
		s, labelsPool, err = UnmarshalTags(labelsPool, s, noEscapes)
		if err != nil {
			return labelsPool, fmt.Errorf("cannot unmarshal labels: %w", err)
		}
		if len(s) > 0 && s[0] == ' ' {
			// Fast path - skip whitespace.
			s = s[1:]
		}
		labels := labelsPool[labelsStart:]
		r.Labels = labels[:len(labels):len(labels)]
	} else {
		// Labels weren't found. Search for value after whitespace
		n = nextWhitespace(s)
		if n < 0 {
			return labelsPool, fmt.Errorf("missing value")
		}
		r.Metric = s[:n]
		s = s[n+1:]
	}
	if len(r.Metric) == 0 {
		return labelsPool, fmt.Errorf("metric cannot be empty")
	}
	s = skipLeadingWhitespace(s)
	if len(s) == 0 {
		return labelsPool, fmt.Errorf("value cannot be empty")
	}
	n = nextWhitespace(s)
	if n < 0 {
		// There is no timestamp.
		v, err := unescapeValue(s)
		if err != nil {
			return nil, fmt.Errorf("unexpected value %q", s)
		}
		r.Value = v
		return labelsPool, nil
	}
	// There is timestamp.
	v, err := unescapeValue(s[:n])
	if err != nil {
		return nil, fmt.Errorf("unexpected value %q", s)
	}
	r.Value = v
	s = skipLeadingWhitespace(s[n+1:])
	ts, err := fastfloat.ParseInt64(bytesutil.ToUnsafeString(s))
	if err != nil {
		return labelsPool, fmt.Errorf("cannot parse timestamp %q: %w", s, err)
	}
	r.Timestamp = ts
	return labelsPool, nil
}

var rowsReadScrape = metrics.NewCounter(`vm_protoparser_rows_read_total{type="promscrape"}`)

func unmarshalRows(dst []Row, s []byte, labelsPool []storage.Label, noEscapes bool, errLogger func(s string)) ([]Row, []storage.Label) {
	dstLen := len(dst)
	for len(s) > 0 {
		n := bytes.IndexByte(s, '\n')
		if n < 0 {
			// The last line.
			dst, labelsPool = unmarshalRow(dst, s, labelsPool, noEscapes, errLogger)
			break
		}
		dst, labelsPool = unmarshalRow(dst, s[:n], labelsPool, noEscapes, errLogger)
		s = s[n+1:]
	}
	rowsReadScrape.Add(len(dst) - dstLen)
	return dst, labelsPool
}

func unmarshalRow(dst []Row, s []byte, labelsPool []storage.Label, noEscapes bool, errLogger func(s string)) ([]Row, []storage.Label) {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		s = s[:len(s)-1]
	}
	s = skipLeadingWhitespace(s)
	if len(s) == 0 {
		// Skip empty line
		return dst, labelsPool
	}
	if s[0] == '#' {
		// Skip comment
		return dst, labelsPool
	}
	if cap(dst) > len(dst) {
		dst = dst[:len(dst)+1]
	} else {
		dst = append(dst, Row{})
	}
	r := &dst[len(dst)-1]
	var err error
	labelsPool, err = r.unmarshal(s, labelsPool, noEscapes)
	if err != nil {
		dst = dst[:len(dst)-1]
		msg := fmt.Sprintf("cannot unmarshal Prometheus line %q: %s", s, err)
		errLogger(msg)
		invalidLines.Inc()
	}
	return dst, labelsPool
}

var invalidLines = metrics.NewCounter(`vm_rows_invalid_total{type="prometheus"}`)

func UnmarshalTags(dst []storage.Label, s []byte, noEscapes bool) ([]byte, []storage.Label, error) {
	for {
		s = skipLeadingWhitespace(s)
		if len(s) > 0 && s[0] == '}' {
			// End of labels found.
			return s[1:], dst, nil
		}
		n := bytes.IndexByte(s, '=')
		if n < 0 {
			return s, dst, fmt.Errorf("missing value for tag %q", s)
		}
		key := skipTrailingWhitespace(s[:n])
		s = skipLeadingWhitespace(s[n+1:])
		if len(s) == 0 || s[0] != '"' {
			return s, dst, fmt.Errorf("expecting quoted value for tag %q; got %q", key, s)
		}
		value := s[1:]
		if noEscapes {
			// Fast path - the line has no escape chars
			n = bytes.IndexByte(value, '"')
			if n < 0 {
				return s, dst, fmt.Errorf("missing closing quote for tag value %q", s)
			}
			s = value[n+1:]
			value = value[:n]
		} else {
			// Slow path - the line contains escape chars
			n = findClosingQuote(s)
			if n < 0 {
				return s, dst, fmt.Errorf("missing closing quote for tag value %q", s)
			}
			var err error
			value, err = unescapeValue(s[:n+1])
			if err != nil {
				return s, dst, fmt.Errorf("cannot unescape value %q for tag %q: %w", s[:n+1], key, err)
			}
			s = s[n+1:]
		}
		if len(key) > 0 {
			// Allow empty values (len(value)==0) - see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/453
			if cap(dst) > len(dst) {
				dst = dst[:len(dst)+1]
			} else {
				dst = append(dst, storage.Label{})
			}
			tag := &dst[len(dst)-1]
			tag.Name = key
			tag.Value = value
		}
		s = skipLeadingWhitespace(s)
		if len(s) > 0 && s[0] == '}' {
			// End of labels found.
			return s[1:], dst, nil
		}
		if len(s) == 0 || s[0] != ',' {
			return s, dst, fmt.Errorf("missing comma after tag %s=%q", key, value)
		}
		s = s[1:]
	}
}

func findClosingQuote(s []byte) int {
	if len(s) == 0 || s[0] != '"' {
		return -1
	}
	off := 1
	s = s[1:]
	for {
		n := bytes.IndexByte(s, '"')
		if n < 0 {
			return -1
		}
		if prevBackslashesCount(s[:n])%2 == 0 {
			return off + n
		}
		off += n + 1
		s = s[n+1:]
	}
}

func unescapeValue(s []byte) ([]byte, error) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return nil, fmt.Errorf("unexpected label value: %q", s)
	}
	n := bytes.IndexByte(s, '\\')
	if n < 0 {
		// Fast path - nothing to unescape
		return s[1 : len(s)-1], nil
	}
	r, err := strconv.Unquote(bytesutil.ToUnsafeString(s))
	if err != nil {
		return nil, err
	}
	return bytesutil.ToUnsafeBytes(r), nil
}

func prevBackslashesCount(s []byte) int {
	n := 0
	for len(s) > 0 && s[len(s)-1] == '\\' {
		n++
		s = s[:len(s)-1]
	}
	return n
}
