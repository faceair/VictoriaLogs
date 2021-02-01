package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

const (
	globalSilent = "s"
)

var (
	globalFlags = []cli.Flag{
		&cli.BoolFlag{
			Name:  globalSilent,
			Value: false,
			Usage: "Whether to run in silent mode. If set to true no confirmation prompts will appear.",
		},
	}
)

const (
	vmAddr               = "vm-addr"
	vmUser               = "vm-user"
	vmPassword           = "vm-password"
	vmAccountID          = "vm-account-id"
	vmConcurrency        = "vm-concurrency"
	vmCompress           = "vm-compress"
	vmBatchSize          = "vm-batch-size"
	vmSignificantFigures = "vm-significant-figures"
	vmRoundDigits        = "vm-round-digits"
	vmExtraLabel         = "vm-extra-label"
)

var (
	vmFlags = []cli.Flag{
		&cli.StringFlag{
			Name:  vmAddr,
			Value: "http://localhost:8428",
			Usage: "VictoriaMetrics address to perform import requests. \n" +
				"Should be the same as --httpListenAddr value for single-node version or VMInsert component. \n" +
				"Please note, that `vmctl` performs initial readiness check for the given address by checking `/health` endpoint.",
		},
		&cli.StringFlag{
			Name:    vmUser,
			Usage:   "VictoriaMetrics username for basic auth",
			EnvVars: []string{"VM_USERNAME"},
		},
		&cli.StringFlag{
			Name:    vmPassword,
			Usage:   "VictoriaMetrics password for basic auth",
			EnvVars: []string{"VM_PASSWORD"},
		},
		&cli.StringFlag{
			Name: vmAccountID,
			Usage: "AccountID is an arbitrary 32-bit integer identifying namespace for data ingestion (aka tenant). \n" +
				"It is possible to set it as accountID:projectID, where projectID is also arbitrary 32-bit integer. \n" +
				"If projectID isn't set, then it equals to 0",
		},
		&cli.UintFlag{
			Name:  vmConcurrency,
			Usage: "Number of workers concurrently performing import requests to VM",
			Value: 2,
		},
		&cli.BoolFlag{
			Name:  vmCompress,
			Value: true,
			Usage: "Whether to apply gzip compression to import requests",
		},
		&cli.IntFlag{
			Name:  vmBatchSize,
			Value: 200e3,
			Usage: "How many samples importer collects before sending the import request to VM",
		},
		&cli.IntFlag{
			Name:  vmSignificantFigures,
			Value: 0,
			Usage: "The number of significant figures to leave in metric values before importing. " +
				"See https://en.wikipedia.org/wiki/Significant_figures. Zero value saves all the significant figures. " +
				"This option may be used for increasing on-disk compression level for the stored metrics. " +
				"See also --vm-round-digits option",
		},
		&cli.IntFlag{
			Name:  vmRoundDigits,
			Value: 100,
			Usage: "Round metric values to the given number of decimal digits after the point. " +
				"This option may be used for increasing on-disk compression level for the stored metrics",
		},
		&cli.StringSliceFlag{
			Name:  vmExtraLabel,
			Value: nil,
			Usage: "Extra labels, that will be added to imported timeseries. In case of collision, label value defined by flag" +
				"will have priority. Flag can be set multiple times, to add few additional labels.",
		},
	}
)

const (
	influxAddr                      = "influx-addr"
	influxUser                      = "influx-user"
	influxPassword                  = "influx-password"
	influxDB                        = "influx-database"
	influxRetention                 = "influx-retention-policy"
	influxChunkSize                 = "influx-chunk-size"
	influxConcurrency               = "influx-concurrency"
	influxFilterSeries              = "influx-filter-series"
	influxFilterTimeStart           = "influx-filter-time-start"
	influxFilterTimeEnd             = "influx-filter-time-end"
	influxMeasurementFieldSeparator = "influx-measurement-field-separator"
)

var (
	influxFlags = []cli.Flag{
		&cli.StringFlag{
			Name:  influxAddr,
			Value: "http://localhost:8086",
			Usage: "Influx server addr",
		},
		&cli.StringFlag{
			Name:    influxUser,
			Usage:   "Influx user",
			EnvVars: []string{"INFLUX_USERNAME"},
		},
		&cli.StringFlag{
			Name:    influxPassword,
			Usage:   "Influx user password",
			EnvVars: []string{"INFLUX_PASSWORD"},
		},
		&cli.StringFlag{
			Name:     influxDB,
			Usage:    "Influx database",
			Required: true,
		},
		&cli.StringFlag{
			Name:  influxRetention,
			Usage: "Influx retention policy",
			Value: "autogen",
		},
		&cli.IntFlag{
			Name:  influxChunkSize,
			Usage: "The chunkSize defines max amount of series to be returned in one chunk",
			Value: 10e3,
		},
		&cli.IntFlag{
			Name:  influxConcurrency,
			Usage: "Number of concurrently running fetch queries to InfluxDB",
			Value: 1,
		},
		&cli.StringFlag{
			Name: influxFilterSeries,
			Usage: "Influx filter expression to select series. E.g. \"from cpu where arch='x86' AND hostname='host_2753'\".\n" +
				"See for details https://docs.influxdata.com/influxdb/v1.7/query_language/schema_exploration#show-series",
		},
		&cli.StringFlag{
			Name:  influxFilterTimeStart,
			Usage: "The time filter to select timeseries with timestamp equal or higher than provided value. E.g. '2020-01-01T20:07:00Z'",
		},
		&cli.StringFlag{
			Name:  influxFilterTimeEnd,
			Usage: "The time filter to select timeseries with timestamp equal or lower than provided value. E.g. '2020-01-01T20:07:00Z'",
		},
		&cli.StringFlag{
			Name:  influxMeasurementFieldSeparator,
			Usage: "The {separator} symbol used to concatenate {measurement} and {field} names into series name {measurement}{separator}{field}.",
			Value: "_",
		},
	}
)

const (
	promSnapshot         = "prom-snapshot"
	promConcurrency      = "prom-concurrency"
	promFilterTimeStart  = "prom-filter-time-start"
	promFilterTimeEnd    = "prom-filter-time-end"
	promFilterLabel      = "prom-filter-label"
	promFilterLabelValue = "prom-filter-label-value"
)

var (
	promFlags = []cli.Flag{
		&cli.StringFlag{
			Name:     promSnapshot,
			Usage:    "Path to Prometheus snapshot. Pls see for details https://www.robustperception.io/taking-snapshots-of-prometheus-data",
			Required: true,
		},
		&cli.IntFlag{
			Name:  promConcurrency,
			Usage: "Number of concurrently running snapshot readers",
			Value: 1,
		},
		&cli.StringFlag{
			Name:  promFilterTimeStart,
			Usage: "The time filter in RFC3339 format to select timeseries with timestamp equal or higher than provided value. E.g. '2020-01-01T20:07:00Z'",
		},
		&cli.StringFlag{
			Name:  promFilterTimeEnd,
			Usage: "The time filter in RFC3339 format to select timeseries with timestamp equal or lower than provided value. E.g. '2020-01-01T20:07:00Z'",
		},
		&cli.StringFlag{
			Name:  promFilterLabel,
			Usage: "Prometheus label name to filter timeseries by. E.g. '__name__' will filter timeseries by name.",
		},
		&cli.StringFlag{
			Name:  promFilterLabelValue,
			Usage: fmt.Sprintf("Prometheus regular expression to filter label from %q flag.", promFilterLabel),
			Value: ".*",
		},
	}
)

const (
	vmNativeFilterMatch     = "vm-native-filter-match"
	vmNativeFilterTimeStart = "vm-native-filter-time-start"
	vmNativeFilterTimeEnd   = "vm-native-filter-time-end"

	vmNativeSrcAddr     = "vm-native-src-addr"
	vmNativeSrcUser     = "vm-native-src-user"
	vmNativeSrcPassword = "vm-native-src-password"

	vmNativeDstAddr     = "vm-native-dst-addr"
	vmNativeDstUser     = "vm-native-dst-user"
	vmNativeDstPassword = "vm-native-dst-password"
)

var (
	vmNativeFlags = []cli.Flag{
		&cli.StringFlag{
			Name: vmNativeFilterMatch,
			Usage: "Time series selector to match series for export. For example, select {instance!=\"localhost\"} will " +
				"match all series with \"instance\" label different to \"localhost\".\n" +
				" See more details here https://github.com/VictoriaMetrics/VictoriaMetrics#how-to-export-data-in-native-format",
			Value: `{__name__!=""}`,
		},
		&cli.StringFlag{
			Name:  vmNativeFilterTimeStart,
			Usage: "The time filter may contain either unix timestamp in seconds or RFC3339 values. E.g. '2020-01-01T20:07:00Z'",
		},
		&cli.StringFlag{
			Name:  vmNativeFilterTimeEnd,
			Usage: "The time filter may contain either unix timestamp in seconds or RFC3339 values. E.g. '2020-01-01T20:07:00Z'",
		},
		&cli.StringFlag{
			Name: vmNativeSrcAddr,
			Usage: "VictoriaMetrics address to perform export from. \n" +
				" Should be the same as --httpListenAddr value for single-node version or VMSelect component." +
				" If exporting from cluster version - include the tenet token in address.",
			Required: true,
		},
		&cli.StringFlag{
			Name:    vmNativeSrcUser,
			Usage:   "VictoriaMetrics username for basic auth",
			EnvVars: []string{"VM_NATIVE_SRC_USERNAME"},
		},
		&cli.StringFlag{
			Name:    vmNativeSrcPassword,
			Usage:   "VictoriaMetrics password for basic auth",
			EnvVars: []string{"VM_NATIVE_SRC_PASSWORD"},
		},
		&cli.StringFlag{
			Name: vmNativeDstAddr,
			Usage: "VictoriaMetrics address to perform import to. \n" +
				" Should be the same as --httpListenAddr value for single-node version or VMInsert component." +
				" If importing into cluster version - include the tenet token in address.",
			Required: true,
		},
		&cli.StringFlag{
			Name:    vmNativeDstUser,
			Usage:   "VictoriaMetrics username for basic auth",
			EnvVars: []string{"VM_NATIVE_DST_USERNAME"},
		},
		&cli.StringFlag{
			Name:    vmNativeDstPassword,
			Usage:   "VictoriaMetrics password for basic auth",
			EnvVars: []string{"VM_NATIVE_DST_PASSWORD"},
		},
		&cli.StringSliceFlag{
			Name:  vmExtraLabel,
			Value: nil,
			Usage: "Extra labels, that will be added to imported timeseries. In case of collision, label value defined by flag" +
				"will have priority. Flag can be set multiple times, to add few additional labels.",
		},
	}
)

func mergeFlags(flags ...[]cli.Flag) []cli.Flag {
	var result []cli.Flag
	for _, f := range flags {
		result = append(result, f...)
	}
	return result
}