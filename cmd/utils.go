package cmd

import (
	"fmt"
	"runtime"

	ver "github.com/miaojuncn/etcd-ops/pkg/version"
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

//
// func BuildRestorerAndStore(opts *restoreOptions) (*restorer.Restorer, error) {
//
// 	clusterUrlsMap, err := etypes.NewURLsMap(opts.restoreConfig.InitialCluster)
// 	if err != nil {
// 		zap.S().Fatalf("Failed creating url map for restore cluster: %v", err)
// 	}
//
// 	peerUrls, err := etypes.NewURLs(opts.restoreConfig.InitialAdvertisePeerURLs)
// 	if err != nil {
// 		zap.S().Fatalf("Failed parsing peers urls for restore cluster: %v", err)
// 	}
//
// 	s, err := store.GetStore(opts.storeConfig)
// 	if err != nil {
// 		zap.S().Fatalf("Failed to create restore snap store from configured storage provider: %v", err)
// 	}
//
// 	zap.S().Info("Finding latest set of snapshot to recover from...")
// 	baseSnap, deltaSnapList, err := tools.GetLatestFullSnapshotAndDeltaSnapList(s)
// 	if err != nil {
// 		zap.S().Fatalf("Failed to get latest snapshot: %v", err)
// 	}
//
// 	if baseSnap == nil {
// 		zap.S().Infof("No base snapshot found. Will do nothing.")
// 		return nil, fmt.Errorf("no base snapshot found")
// 	}
//
// 	return &restorer.Restorer{
// 		Config:        opts.restoreConfig,
// 		Store:         s,
// 		BaseSnapshot:  baseSnap,
// 		DeltaSnapList: deltaSnapList,
// 		ClusterURLs:   clusterUrlsMap,
// 		PeerURLs:      peerUrls,
// 	}, nil
// }
