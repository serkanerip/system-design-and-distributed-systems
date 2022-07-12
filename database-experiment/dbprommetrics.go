package databaseexperiment

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	pmTotalWrites     prometheus.Counter
	pmTotalReads      prometheus.Counter
	pmTotalCompaction prometheus.Counter
	pmTotalMerge      prometheus.Counter
	pmSegmentCount    prometheus.Gauge
)

func init() {
	pmTotalWrites = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "expdb_total_writes",
		Help:        "Total number of Write operations.",
		ConstLabels: prometheus.Labels{"app": "dbexp"},
	})

	pmTotalReads = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "expdb_total_reads",
		Help:        "Total number of Read operations.",
		ConstLabels: prometheus.Labels{"app": "dbexp"},
	})

	pmTotalCompaction = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "expdb_total_compaction",
		Help:        "Total number of Compaction operations.",
		ConstLabels: prometheus.Labels{"app": "dbexp"},
	})

	pmTotalCompaction = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "expdb_total_merge",
		Help:        "Total number of Merge operations.",
		ConstLabels: prometheus.Labels{"app": "dbexp"},
	})

	pmSegmentCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "exdb_segment_count",
		Help:        "Current number of Immutable segments.",
		ConstLabels: prometheus.Labels{"app": "dbexp"},
	})
}
