package importer

import (
	"bytes"
	"io"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vminsert/relabel"
	parser "github.com/VictoriaMetrics/VictoriaLogs/lib/protoparser/importer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson/fastfloat"
)

var (
	rowsInserted  = tenantmetrics.NewCounterMap(`vm_rows_inserted_total{type="importer"}`)
	rowsPerInsert = metrics.NewHistogram(`vm_rows_per_insert{type="importer"}`)
)

// InsertHandler processes remote write for plaintext protocol.
func InsertHandler(at *auth.Token, r io.Reader) error {
	return writeconcurrencylimiter.Do(func() error {
		return parser.ParseStream(r, func(rows []parser.Row) error {
			return insertRows(at, rows)
		})
	})
}

func insertRows(at *auth.Token, rows []parser.Row) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	atCopy := *at
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		ctx.AddLabel(nil, r.Metric)
		for j := range r.Labels {
			label := &r.Labels[j]
			if atCopy.AccountID == 0 {
				// Multi-tenancy support via custom tags.
				// Do not allow overriding AccountID and ProjectID from atCopy for security reasons.
				if bytes.Equal(label.Name, []byte("VictoriaMetrics_AccountID")) {
					atCopy.AccountID = uint32(fastfloat.ParseUint64BestEffort(bytesutil.ToUnsafeString(label.Value)))
				}
				if atCopy.ProjectID == 0 && bytes.Equal(label.Name, []byte("VictoriaMetrics_ProjectID")) {
					atCopy.ProjectID = uint32(fastfloat.ParseUint64BestEffort(bytesutil.ToUnsafeString(label.Value)))
				}
			}
			ctx.AddLabel(label.Name, label.Value)
		}
		if hasRelabeling {
			ctx.ApplyRelabeling()
		}
		if len(ctx.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		if err := ctx.WriteDataPoint(&atCopy, ctx.Labels, r.Timestamp, r.Value); err != nil {
			return err
		}
	}
	// Assume that all the rows for a single connection belong to the same (AccountID, ProjectID).
	rowsInserted.Get(&atCopy).Add(len(rows))
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}
