package cmd

import (
	"fmt"
	"runtime"

	"github.com/miaojuncn/etcd-ops/pkg/types"
	ver "github.com/miaojuncn/etcd-ops/pkg/version"
	etypes "go.etcd.io/etcd/client/pkg/v3/types"
)

var (
	version bool
)

func printVersionInfo() {
	fmt.Printf("etcd-ops Version: %s\n", ver.Version)
	fmt.Printf("GIT SHA: %s\n", ver.GitSHA)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func BuildRestoreOptionsAndStore(opts *restoreOptions) (*types.RestoreOptions, types.Store, error) {
	if err := opts.validate(); err != nil {
		return nil, nil, err
	}

	opts.complete()

	clusterUrlsMap, err := etypes.NewURLsMap(opts.restorationConfig.InitialCluster)
	if err != nil {
		logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	peerUrls, err := etypes.NewURLs(opts.restorationConfig.InitialAdvertisePeerURLs)
	if err != nil {
		logger.Fatalf("failed parsing peers urls for restore cluster: %v", err)
	}

	store, err := snapstore.GetSnapstore(opts.snapstoreConfig)
	if err != nil {
		logger.Fatalf("failed to create restore snapstore from configured storage provider: %v", err)
	}

	logger.Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		logger.Fatalf("failed to get latest snapshot: %v", err)
	}

	if baseSnap == nil {
		logger.Infof("No base snapshot found. Will do nothing.")
		return nil, nil, fmt.Errorf("no base snapshot found")
	}

	return &brtypes.RestoreOptions{
		Config:        opts.restorationConfig,
		BaseSnapshot:  baseSnap,
		DeltaSnapList: deltaSnapList,
		ClusterURLs:   clusterUrlsMap,
		PeerURLs:      peerUrls,
	}, store, nil
}
