package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// LabelSucceeded is a metric label indicating whether associated metric
	// series is for success or failure.
	LabelSucceeded = "succeeded"
	// ValueSucceededTrue is value True for metric label succeeded.
	ValueSucceededTrue = "true"
	// ValueSucceededFalse is value False for metric label failed.
	ValueSucceededFalse = "false"
	// LabelKind is a metrics label indicates kind of snapshot associated with metric.
	LabelKind = "kind"
	// LabelError is a metric error to indicate error occurred.
	LabelError = "error"
	// LabelEndPoint is metric label for metric of etcd cluster endpoint.
	LabelEndPoint = "endpoint"
)

var (
	// GCSnapshotCounter is metric to count the garbage collected snapshots.
	GCSnapshotCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gc_total",
			Help: "Total number of garbage collected snapshots.",
		},
		[]string{LabelKind, LabelSucceeded},
	)

	// LatestSnapshotRevision is metric to expose the latest snapshot revision.
	LatestSnapshotRevision = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "latest_revision",
			Help: "Revision number of latest snapshot taken.",
		},
		[]string{LabelKind},
	)

	// LatestSnapshotTimestamp is metric to expose the latest snapshot timestamp.
	LatestSnapshotTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "latest_timestamp",
			Help: "Timestamp of latest snapshot taken.",
		},
		[]string{LabelKind},
	)

	// SnapshotRequired is metric to expose snapshot required flag.
	SnapshotRequired = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "required",
			Help: "Indicates whether a snapshot is required to be taken.",
		},
		[]string{LabelKind},
	)

	// SnapshotDurationSeconds is metric to expose the duration required to save snapshot in seconds.
	SnapshotDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "duration_seconds",
			Help: "Total latency distribution of saving snapshot to object store.",
		},
		[]string{LabelKind, LabelSucceeded},
	)

	// DefragDurationSeconds is metric to expose duration required to defrag all the members of etcd cluster.
	DefragDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "defrag_duration_seconds",
			Help: "Total latency distribution of defragmentation of each etcd cluster member.",
		},
		[]string{LabelSucceeded, LabelEndPoint},
	)

	// StoreLatestDeltasTotal is metric to expose total number of delta snapshots taken since the latest full snapshot.
	StoreLatestDeltasTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "latest_deltas_total",
			Help: "Total number of delta snapshots taken since the latest full snapshot.",
		},
		[]string{},
	)
	// StoreLatestDeltasRevisionsTotal is metric to expose total number of revisions stored in delta snapshots taken since the latest full snapshot.
	StoreLatestDeltasRevisionsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "latest_deltas_revisions_total",
			Help: "Total number of revisions stored in delta snapshots taken since the latest full snapshot.",
		},
		[]string{},
	)

	// SnapActionOperationFailure is metric to count the number of snapaction operations that have errored out
	SnapActionOperationFailure = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failure",
			Help: "Total number of snapaction errors.",
		},
		[]string{LabelError},
	)
)

func init() {
	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(GCSnapshotCounter)

	prometheus.MustRegister(LatestSnapshotRevision)
	prometheus.MustRegister(LatestSnapshotTimestamp)
	prometheus.MustRegister(SnapshotRequired)

	prometheus.MustRegister(SnapshotDurationSeconds)
	prometheus.MustRegister(DefragDurationSeconds)

	prometheus.MustRegister(StoreLatestDeltasTotal)
	prometheus.MustRegister(StoreLatestDeltasRevisionsTotal)

	prometheus.MustRegister(SnapActionOperationFailure)

}
