package tools

import (
	"sort"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/snapshot"
	"github.com/miaojuncn/etcd-ops/pkg/snaptaker"
	"github.com/miaojuncn/etcd-ops/pkg/store"
)

const (
	timeTemplate = "20060102"
)

func UnixTime2String(t time.Time) string {
	return t.Format(timeTemplate)
}

// GetLatestFullSnapshotAndDeltaSnapList returns the latest snapshot
func GetLatestFullSnapshotAndDeltaSnapList(store store.Store) (*snapshot.Snapshot, snapshot.SnapList, error) {
	var (
		fullSnapshot  *snapshot.Snapshot
		deltaSnapList snapshot.SnapList
	)
	snapList, err := store.List()
	if err != nil {
		return nil, nil, err
	}

	for index := len(snapList); index > 0; index-- {
		if snapList[index-1].IsChunk {
			continue
		}
		if snapList[index-1].Kind == snaptaker.SnapshotKindFull {
			fullSnapshot = snapList[index-1]
			break
		}
		deltaSnapList = append(deltaSnapList, snapList[index-1])
	}

	sort.Sort(deltaSnapList)
	return fullSnapshot, deltaSnapList, nil
}
