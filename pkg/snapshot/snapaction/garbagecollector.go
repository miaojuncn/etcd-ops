package snapaction

import (
	"path"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/metrics"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/prometheus/client_golang/prometheus"
)

// RunGarbageCollector basically consider the older backups as garbage and deletes it
func (sa *SnapAction) RunGarbageCollector(stopCh <-chan struct{}) {
	if sa.policy.GarbageCollectionPeriod <= time.Second {
		zlog.Logger.Infof("GC: Not running garbage collector since GarbageCollectionPeriod [%s] set to less than 1 second.",
			sa.policy.GarbageCollectionPeriod)
		return
	}

	GCTicker := time.NewTicker(sa.policy.GarbageCollectionPeriod)
	defer GCTicker.Stop()
	for {
		select {
		case <-stopCh:
			zlog.Logger.Info("GC: Stop signal received. Closing garbage collector.")
			return
		case <-GCTicker.C:

			var err error
			// Update the snap store object before taking any action on object storage bucket.
			sa.store, err = store.GetStore(sa.storeConfig)
			if err != nil {
				zlog.Logger.Warnf("GC: Failed to create snap store from configured storage provider: %v", err)
				continue
			}

			total := 0
			zlog.Logger.Info("GC: Executing garbage collection...")
			snapList, err := sa.store.List()
			if err != nil {
				zlog.Logger.Warnf("GC: Failed to list snapshots: %v", err)
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
						zlog.Logger.Infof("GC: Deleting old full snapshot: %s", snapPath)
						if err := sa.store.Delete(*snap); err != nil {
							zlog.Logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
							metrics.SnapActionOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
							metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
							continue
						}
						metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
						total++
						garbageCollectChunks(sa.store, snapList, snapStreamIndexList[snapStreamIndex]+1, snapStreamIndexList[snapStreamIndex+1])
					}
				}
			case types.GarbageCollectionPolicyKeepAlways:

			}
			zlog.Logger.Infof("GC: Total number garbage collected snapshots: %d", total)
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

// garbageCollectChunks deletes the chunks in the store from snap list starting at index low (inclusive) till high (exclusive).
func garbageCollectChunks(store types.Store, snapList types.SnapList, low, high int) {
	for index := low; index < high; index++ {
		snap := snapList[index]
		// Only delete chunk snapshots of kind Full
		if snap.Kind != types.SnapshotKindFull {
			continue
		}
		snapPath := path.Join(snap.SnapDir, snap.SnapName)
		zlog.Logger.Infof("GC: Deleting chunk for old full snapshot: %s", snapPath)
		if err := store.Delete(*snap); err != nil {
			zlog.Logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
			continue
		}
	}
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
		zlog.Logger.Infof("GC: Deleting old delta snapshot: %s", snapPath)
		if err := sa.store.Delete(*snapStream[i]); err != nil {
			zlog.Logger.Warnf("GC: Failed to delete snapshot %s: %v", snapPath, err)
			metrics.SnapActionOperationFailure.With(prometheus.Labels{metrics.LabelError: err.Error()}).Inc()
			metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Inc()
			return total, err
		}
		metrics.GCSnapshotCounter.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Inc()
		total++
	}
	return total, nil
}
