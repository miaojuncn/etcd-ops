package snapaction

import (
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/metrics"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
)

// RunGarbageCollector basically consider the older backups as garbage and deletes it
func (sa *SnapAction) RunGarbageCollector(stopCh <-chan struct{}) {
	if sa.policy.GarbageCollectionPeriod <= time.Second {
		sa.logger.Info("GC: Not running garbage collector since GarbageCollectionPeriod set to less than 1 second.",
			zap.Duration("period", sa.policy.GarbageCollectionPeriod))
		return
	}

	GCTicker := time.NewTicker(sa.policy.GarbageCollectionPeriod)
	defer GCTicker.Stop()
	for {
		select {
		case <-stopCh:
			sa.logger.Info("GC: Stop signal received. Closing garbage collector.")
			return
		case <-GCTicker.C:

			var err error
			// Update the snap store object before taking any action on object storage bucket.
			sa.store, err = store.GetStore(sa.storeConfig)
			if err != nil {
				sa.logger.Warn("GC: Failed to create snap store from configured storage provider.", zap.NamedError("error", err))
				continue
			}

			total := 0
			sa.logger.Info("GC: Executing garbage collection.")
			snapList, err := sa.store.List()
			if err != nil {
				sa.logger.Warn("GC: Failed to list snapshots.", zap.NamedError("error", err))
				continue
			}

			snapStreamIndexList := getSnapStreamIndexList(snapList)

			switch sa.policy.GarbageCollectionPolicy {
			case types.GarbageCollectionPolicyLimitBased:
				// Delete delta snapshots in all snapStream but the latest one.
				// Delete all snapshots beyond limit set by sa.policy.maxBackups.
				for snapStreamIndex := 0; snapStreamIndex < len(snapStreamIndexList)-1; snapStreamIndex++ {
					deletedSnap, err := sa.garbageCollectDeltaSnapshots(snapList[snapStreamIndexList[snapStreamIndex]:snapStreamIndexList[snapStreamIndex+1]])
					total += deletedSnap
					if err != nil {
						continue
					}
					if snapStreamIndex < len(snapStreamIndexList)-int(sa.policy.MaxBackups) {
						snap := snapList[snapStreamIndexList[snapStreamIndex]]
						snapPath := path.Join(snap.SnapDir, snap.SnapName)
						sa.logger.Info("GC: Deleting old full snapshot.", zap.String("filepath", snapPath))
						if err := sa.store.Delete(*snap); err != nil {
							sa.logger.Warn("GC: Failed to delete snapshot.",
								zap.String("filepath", snapPath), zap.NamedError("error", err))
							metrics.SnapActionOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
							metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
							continue
						}
						metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
						total++
					}
				}
			case types.GarbageCollectionPolicyKeepAlways:

			}
			sa.logger.Info("GC: Total number garbage collected snapshots.", zap.Int("total", total))
		}
	}
}

// getSnapStreamIndexList lists the index of snapStreams in snapList which consist of collection of snapStream.
// snapStream indicates the list of snapshot, where first snapshot is base/full snapshot followed by
// list of incremental snapshots based on it.
func getSnapStreamIndexList(snapList types.SnapList) []int {
	// At this stage, we assume the snapList is sorted in increasing order of last revision number, i.e. snapshot with lower
	// last revision at lower index and snapshot with higher last revision at higher index in list.
	var snapStreamIndexList []int
	snapStreamIndexList = append(snapStreamIndexList, 0)
	for index := 1; index < len(snapList); index++ {
		if snapList[index].Kind == types.SnapshotKindFull {
			snapStreamIndexList = append(snapStreamIndexList, index)
		}
	}
	return snapStreamIndexList
}

// garbageCollectDeltaSnapshots deletes only the delta snapshots from revision sorted <snapStream>. It won't delete the full snapshot
// in snap stream which supposed to be at index 0 in <snapStream>.
func (sa *SnapAction) garbageCollectDeltaSnapshots(snapStream types.SnapList) (int, error) {
	total := 0
	for i := len(snapStream) - 1; i > 0; i-- {
		if (*snapStream[i]).Kind != types.SnapshotKindDelta {
			continue
		}
		snapPath := path.Join(snapStream[i].SnapDir, snapStream[i].SnapName)
		sa.logger.Info("GC: Deleting old delta snapshot.", zap.String("filepath", snapPath))
		if err := sa.store.Delete(*snapStream[i]); err != nil {
			sa.logger.Warn("GC: Failed to delete snapshot.",
				zap.String("filepath", snapPath), zap.NamedError("error", err))
			metrics.SnapActionOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
			metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
			return total, err
		}
		metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
		total++
	}
	return total, nil
}
