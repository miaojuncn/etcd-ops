package tools

import (
	"context"
	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/metrics"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/prometheus/client_golang/prometheus"
	"sort"
)

// GetLatestFullSnapshotAndDeltaSnapList returns the latest snapshot
func GetLatestFullSnapshotAndDeltaSnapList(store types.Store) (*types.Snapshot, types.SnapList, error) {
	var (
		fullSnapshot  *types.Snapshot
		deltaSnapList types.SnapList
	)
	snapList, err := store.List()
	if err != nil {
		return nil, nil, err
	}

	for index := len(snapList); index > 0; index-- {
		if snapList[index-1].Kind == types.SnapshotKindFull {
			fullSnapshot = snapList[index-1]
			break
		}
		deltaSnapList = append(deltaSnapList, snapList[index-1])
	}

	sort.Sort(deltaSnapList)
	if len(deltaSnapList) == 0 {
		metrics.StoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)
	} else {
		revisionDiff := deltaSnapList[len(deltaSnapList)-1].LastRevision - deltaSnapList[0].StartRevision
		metrics.StoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(float64(revisionDiff))
	}
	return fullSnapshot, deltaSnapList, nil
}

// GetAllEtcdEndpoints returns the endPoints of all etcd-member.
func GetAllEtcdEndpoints(ctx context.Context, client client.ClusterCloser, etcdConnectionConfig *types.EtcdConnectionConfig) ([]string, error) {
	var etcdEndpoints []string

	ctx, cancel := context.WithTimeout(ctx, etcdConnectionConfig.ConnectionTimeout)
	defer cancel()

	membersInfo, err := client.MemberList(ctx)
	if err != nil {
		zlog.Logger.Errorf("Failed to get memberList of etcd with error: %v", err)
		return nil, err
	}

	for _, member := range membersInfo.Members {
		etcdEndpoints = append(etcdEndpoints, member.GetClientURLs()...)
	}

	return etcdEndpoints, nil
}

// IsEtcdClusterHealthy checks whether all members of etcd cluster are in healthy state or not.
func IsEtcdClusterHealthy(ctx context.Context, client client.MaintenanceCloser, etcdConnectionConfig *types.EtcdConnectionConfig, etcdEndpoints []string) (bool, error) {

	for _, endPoint := range etcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(ctx, etcdConnectionConfig.ConnectionTimeout)
			defer cancel()
			if _, err := client.Status(ctx, endPoint); err != nil {
				zlog.Logger.Errorf("Failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
				return err
			}
			return nil
		}(); err != nil {
			return false, err
		}
	}

	return true, nil
}
