package remotewrite

import (
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/lokipb"
	importerParser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/importer"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/remotewrite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted  = tenantmetrics.NewCounterMap(`vm_rows_inserted_total{type="promremotewrite"}`)
	rowsPerInsert = metrics.NewHistogram(`vm_rows_per_insert{type="promremotewrite"}`)
)

// InsertHandler processes remote write for prometheus.
func InsertHandler(at *auth.Token, req *http.Request) error {
	return writeconcurrencylimiter.Do(func() error {
		return parser.ParseStream(req, func(timeseries []lokipb.Stream) error {
			return insertRows(at, timeseries)
		})
	})
}

func insertRows(at *auth.Token, timeseries []lokipb.Stream) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	rowsTotal := 0
	hasRelabeling := relabel.HasRelabeling()
	for i := range timeseries {
		ts := &timeseries[i]
		ctx.Labels = ctx.Labels[:0]

		noEscapes := strings.IndexByte(ts.Labels, '\\') < 0
		tail, _, err := importerParser.UnmarshalTags(ctx.Labels, bytesutil.ToUnsafeBytes(ts.Labels), noEscapes)
		if len(tail) > 0 || err != nil {
			continue
		}

		if hasRelabeling {
			ctx.ApplyRelabeling()
		}
		if len(ctx.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		storageNodeIdx := ctx.GetStorageNodeIdx(at, ctx.Labels)
		ctx.MetricNameBuf = ctx.MetricNameBuf[:0]
		entries := ts.Entries
		for i := range entries {
			r := &entries[i]
			if len(ctx.MetricNameBuf) == 0 {
				ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], at.AccountID, at.ProjectID, ctx.Labels)
			}
			if err := ctx.WriteDataPointExt(at, storageNodeIdx, ctx.MetricNameBuf, r.Timestamp.UnixNano()/1e6, bytesutil.ToUnsafeBytes(r.Line)); err != nil {
				return err
			}
		}
		rowsTotal += len(entries)
	}
	rowsInserted.Get(at).Add(rowsTotal)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
