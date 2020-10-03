package main

import (
	"flag"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/graphite"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	graphiteserver "github.com/VictoriaMetrics/VictoriaMetrics/lib/ingestserver/graphite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
)

var (
	graphiteListenAddr     = flag.String("graphiteListenAddr", ":2005", "TCP and UDP address to listen for Graphite plaintext data. Usually :2003 must be set. Doesn't work if empty")
	maxLabelsPerTimeseries = flag.Int("maxLabelsPerTimeseries", 30, "The maximum number of labels accepted per time series. Superflouos labels are dropped")
	storageNodes           = flagutil.NewArray("storageNode", "Address of vmstorage nodes; usage: -storageNode=vmstorage-host1:8400 -storageNode=vmstorage-host2:8400")
)

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	envflag.Parse()
	buildinfo.Init()
	logger.Init()
	cgroup.UpdateGOMAXPROCSToCPUQuota()

	logger.Infof("initializing netstorage for storageNodes %s...", *storageNodes)
	startTime := time.Now()
	if len(*storageNodes) == 0 {
		logger.Fatalf("missing -storageNode arg")
	}
	netstorage.InitStorageNodes(*storageNodes)
	logger.Infof("successfully initialized netstorage in %.3f seconds", time.Since(startTime).Seconds())

	relabel.Init()
	storage.SetMaxLabelsPerTimeseries(*maxLabelsPerTimeseries)
	common.StartUnmarshalWorkers()
	writeconcurrencylimiter.Init()

	graphiteserver.MustStart(*graphiteListenAddr, func(r io.Reader) error {
		var at auth.Token // TODO: properly initialize auth token
		return graphite.InsertHandler(&at, r)
	})

	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)

	startTime = time.Now()
	logger.Infof("successfully shut down http service in %.3f seconds", time.Since(startTime).Seconds())

	common.StopUnmarshalWorkers()

	logger.Infof("shutting down neststorage...")
	startTime = time.Now()
	netstorage.Stop()
	logger.Infof("successfully stopped netstorage in %.3f seconds", time.Since(startTime).Seconds())

	fs.MustStopDirRemover()

	logger.Infof("the vminsert has been stopped")
}

var (
	_ = metrics.NewGauge(`vm_metrics_with_dropped_labels_total`, func() float64 {
		return float64(atomic.LoadUint64(&storage.MetricsWithDroppedLabels))
	})
	_ = metrics.NewGauge(`vm_too_long_label_names_total`, func() float64 {
		return float64(atomic.LoadUint64(&storage.TooLongLabelNames))
	})
	_ = metrics.NewGauge(`vm_too_long_label_values_total`, func() float64 {
		return float64(atomic.LoadUint64(&storage.TooLongLabelValues))
	})
)
