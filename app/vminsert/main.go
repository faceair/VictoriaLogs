package main

import (
	"flag"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/importer"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/remotewrite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
)

var (
	importerListenAddr     = flag.String("importerListenAddr", "", "TCP and UDP address to listen for plaintext data. Usually :2003 must be set. Doesn't work if empty")
	httpListenAddr         = flag.String("httpListenAddr", ":8480", "Address to listen for http connections")
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

	if *importerListenAddr != "" {
		importer.MustStart(*importerListenAddr, func(r io.Reader) error {
			var at auth.Token
			return importer.InsertHandler(&at, r)
		})
	}

	go func() {
		httpserver.Serve(*httpListenAddr, requestHandler)
	}()

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

func requestHandler(w http.ResponseWriter, r *http.Request) bool {
	p, err := httpserver.ParsePath(r.URL.Path)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse path %q: %s", r.URL.Path, err)
		return true
	}
	if p.Prefix != "insert" {
		// This is not our link.
		return false
	}
	at, err := auth.NewToken(p.AuthToken)
	if err != nil {
		httpserver.Errorf(w, r, "auth error: %s", err)
		return true
	}

	switch p.Suffix {
	case "loki/api/v1/push":
		prometheusWriteRequests.Inc()
		if err := remotewrite.InsertHandler(at, r); err != nil {
			prometheusWriteErrors.Inc()
			httpserver.Errorf(w, r, "error in %q: %s", r.URL.Path, err)
			return true
		}
		w.WriteHeader(http.StatusNoContent)
		return true
	default:
		// This is not our link
		return false
	}
}

var (
	prometheusWriteRequests = metrics.NewCounter(`vm_http_requests_total{path="/insert/{}/prometheus/", protocol="remotewrite"}`)
	prometheusWriteErrors   = metrics.NewCounter(`vm_http_request_errors_total{path="/insert/{}/prometheus/", protocol="remotewrite"}`)

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
