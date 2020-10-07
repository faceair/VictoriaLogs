package importer

import (
	"bytes"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestParseStream(t *testing.T) {
	common.StartUnmarshalWorkers()
	defer common.StopUnmarshalWorkers()

	f := func(s string, rowsExpected []Row) {
		t.Helper()
		bb := bytes.NewBufferString(s)
		var result []Row
		var lock sync.Mutex
		doneCh := make(chan struct{})
		err := ParseStream(bb, func(rows []Row) error {
			lock.Lock()
			result = appendRowCopies(result, rows)
			if len(result) == len(rowsExpected) {
				close(doneCh)
			}
			lock.Unlock()
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error when parsing %q: %s", s, err)
		}
		select {
		case <-doneCh:
		case <-time.After(time.Second):
			t.Fatalf("timeout")
		}
		sortRows(result)
		if !reflect.DeepEqual(result, rowsExpected) {
			t.Fatalf("unexpected rows parsed; got\n%v\nwant\n%v", result, rowsExpected)
		}
	}

	f("foo \"123\" 456", []Row{{
		Metric:    []byte("foo"),
		Value:     []byte("123"),
		Timestamp: 456,
	}})
	f(`foo{bar="baz"} "1" 2`+"\n"+`aaa{} "3" 4`, []Row{
		{
			Metric:    []byte("aaa"),
			Value:     []byte("3"),
			Timestamp: 4,
		},
		{
			Metric: []byte("foo"),
			Labels: []storage.Label{{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			}},
			Value:     []byte("1"),
			Timestamp: 2,
		},
	})
	f("foo \"23\" 123", []Row{{
		Metric:    []byte("foo"),
		Value:     []byte("23"),
		Timestamp: 123,
	}})
}

func sortRows(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		return string(a.Metric) < string(b.Metric)
	})
}

func appendRowCopies(dst, src []Row) []Row {
	for _, r := range src {
		// Make a copy of r, since r may contain garbage after returning from the callback to ParseStream.
		var rCopy Row
		rCopy.Metric = copyBytes(r.Metric)
		rCopy.Value = r.Value
		rCopy.Timestamp = r.Timestamp
		for _, label := range r.Labels {
			rCopy.Labels = append(rCopy.Labels, storage.Label{
				Name:  copyBytes(label.Name),
				Value: copyBytes(label.Value),
			})
		}
		dst = append(dst, rCopy)
	}
	return dst
}

func copyBytes(s []byte) []byte {
	return append([]byte(nil), s...)
}
