package restorer

import (
	"fmt"

	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	etypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.uber.org/zap"
)

type NewClientFactoryFunc func(cfg types.EtcdConnectionConfig, opts ...client.Option) client.Factory

type Restorer struct {
	Config      *types.RestoreConfig
	Store       types.Store
	ClusterURLs etypes.URLsMap
	// OriginalClusterSize indicates the actual cluster size from the ETCD config
	OriginalClusterSize int
	PeerURLs            etypes.URLs
	// Base full snapshot + delta snapshots to restore from
	BaseSnapshot     *types.Snapshot
	DeltaSnapList    types.SnapList
	NewClientFactory NewClientFactoryFunc
}

func NewRestorer(restoreConfig *types.RestoreConfig, storeConfig *types.StoreConfig) (*Restorer, error) {
	clusterUrlsMap, err := etypes.NewURLsMap(restoreConfig.InitialCluster)
	if err != nil {
		zap.S().Fatalf("Failed creating url map for restore cluster: %v", err)
	}

	peerUrls, err := etypes.NewURLs(restoreConfig.InitialAdvertisePeerURLs)
	if err != nil {
		zap.S().Fatalf("Failed parsing peers urls for restore cluster: %v", err)
	}

	s, err := store.GetStore(storeConfig)
	if err != nil {
		zap.S().Fatalf("Failed to create restore snap store from configured storage provider: %v", err)
	}

	zap.S().Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := tools.GetLatestFullSnapshotAndDeltaSnapList(s)
	if err != nil {
		zap.S().Fatalf("Failed to get latest snapshot: %v", err)
	}

	if baseSnap == nil {
		zap.S().Infof("No base snapshot found. Will do nothing.")
		return nil, fmt.Errorf("no base snapshot found")
	}

	return &Restorer{
		Config:        restoreConfig,
		Store:         s,
		BaseSnapshot:  baseSnap,
		DeltaSnapList: deltaSnapList,
		ClusterURLs:   clusterUrlsMap,
		PeerURLs:      peerUrls,
	}, nil
}
