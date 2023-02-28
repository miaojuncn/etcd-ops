package restorer

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/member"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	etypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
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
		return nil, fmt.Errorf("failed creating url map for restore cluster: %v", err)
	}

	peerUrls, err := etypes.NewURLs(restoreConfig.InitialAdvertisePeerURLs)
	if err != nil {
		return nil, fmt.Errorf("failed parsing peers urls for restore cluster: %v", err)
	}

	s, err := store.GetStore(storeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore snap store from configured storage provider: %v", err)
	}

	zap.S().Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := tools.GetLatestFullSnapshotAndDeltaSnapList(s)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %v", err)
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

// RestoreAndStopEtcd restore the etcd data directory as per specified restore options but doesn't return the ETCD server that it started.
func (r *Restorer) RestoreAndStopEtcd(ro Restorer, m member.Control) error {
	embeddedEtcd, err := r.Restore(ro, m)
	defer func() {
		if embeddedEtcd != nil {
			embeddedEtcd.Server.Stop()
			embeddedEtcd.Close()
		}
	}()
	return err
}

// StartEmbeddedEtcd starts the embedded etcd server.
func StartEmbeddedEtcd(ro *Restorer) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(ro.Config.RestoreDataDir)
	DefaultListenPeerURLs := "http://localhost:0"
	DefaultListenClientURLs := "http://localhost:0"
	DefaultInitialAdvertisePeerURLs := "http://localhost:0"
	DefaultAdvertiseClientURLs := "http://localhost:0"
	lpurl, _ := url.Parse(DefaultListenPeerURLs)
	apurl, _ := url.Parse(DefaultInitialAdvertisePeerURLs)
	lcurl, _ := url.Parse(DefaultListenClientURLs)
	acurl, _ := url.Parse(DefaultAdvertiseClientURLs)
	cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}
	cfg.APUrls = []url.URL{*apurl}
	cfg.ACUrls = []url.URL{*acurl}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.QuotaBackendBytes = ro.Config.EmbeddedEtcdQuotaBytes
	cfg.MaxRequestBytes = ro.Config.MaxRequestBytes
	cfg.MaxTxnOps = ro.Config.MaxTxnOps
	cfg.AutoCompactionMode = ro.Config.AutoCompactionMode
	cfg.AutoCompactionRetention = ro.Config.AutoCompactionRetention
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	select {
	case <-e.Server.ReadyNotify():
		zap.S().Infof("Embedded server is ready to listen client at: %s", e.Clients[0].Addr())
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// Restore the etcd data directory as per specified restore options but returns the ETCD server that it started.
func (r *Restorer) Restore(ro Restorer, m member.Control) (*embed.Etcd, error) {
	if err := r.restoreFromBaseSnapshot(ro); err != nil {
		return nil, fmt.Errorf("failed to restore from the base snapshot :%v", err)
	}
	if len(ro.DeltaSnapList) == 0 {
		zap.S().Info("No delta snapshots present over base snapshot.")
		return nil, nil
	}
	zap.S().Info("Starting embedded etcd server...")
	e, err := StartEmbeddedEtcd(&ro)
	if err != nil {
		return e, err
	}

	clientFactory := etcd.NewClientFactory(ro.NewClientFactory, types.EtcdConnectionConfig{
		MaxCallSendMsgSize: ro.Config.MaxCallSendMsgSize,
		Endpoints:          []string{e.Clients[0].Addr().String()},
		InsecureTransport:  true,
	})
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return e, err
	}
	defer clientKV.Close()

	zap.S().Infof("Applying delta snapshots...")
	if err := r.applyDeltaSnapshots(clientKV, ro); err != nil {
		return e, err
	}

	if m != nil {
		clientCluster, err := clientFactory.NewCluster()
		if err != nil {
			return e, err
		}
		defer clientCluster.Close()
		m.UpdateMemberPeerURL(context.TODO(), clientCluster)
	}
	return e, nil
}

// restoreFromBaseSnapshot restore the etcd data directory from base snapshot.
func (r *Restorer) restoreFromBaseSnapshot(ro Restorer) error {
	var err error
	if path.Join(ro.BaseSnapshot.SnapDir, ro.BaseSnapshot.SnapName) == "" {
		zap.S().Warn("Base snapshot path not provided. Will do nothing.")
		return nil
	}
	zap.S().Infof("Restoring from base snapshot: %s", path.Join(ro.BaseSnapshot.SnapDir, ro.BaseSnapshot.SnapName))
	cfg := embed.Config{
		InitialClusterToken: ro.Config.InitialClusterToken,
		InitialPeerURLsMap:  ro.ClusterURLs,
		PeerURLs:            ro.PeerURLs,
		Name:                ro.Config.Name,
	}
	if err := cfg.VerifyBootstrap(); err != nil {
		return err
	}

	cl, err := membership.NewClusterFromURLsMap(r.zapLogger, ro.Config.InitialClusterToken, ro.ClusterURLs)
	if err != nil {
		return err
	}

	memberDir := filepath.Join(ro.Config.RestoreDataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		return fmt.Errorf("member directory in data directory(%q) exists", memberDir)
	}

	walDir := filepath.Join(memberDir, "wal")
	snapdir := filepath.Join(memberDir, "snap")
	if err = r.makeDB(snapdir, ro.BaseSnapshot, len(cl.Members()), ro.Config.SkipHashCheck); err != nil {
		return err
	}
	return makeWALAndSnap(r.zapLogger, walDir, snapdir, cl, ro.Config.Name)
}

// applyDeltaSnapshots fetches the events from delta snapshots in parallel and applies them to the embedded etcd sequentially.
func (r *Restorer) applyDeltaSnapshots(clientKV client.KVCloser, ro brtypes.RestoreOptions) error {
	snapList := ro.DeltaSnapList
	numMaxFetchers := ro.Config.MaxFetchers

	firstDeltaSnap := snapList[0]

	if err := r.applyFirstDeltaSnapshot(clientKV, *firstDeltaSnap); err != nil {
		return err
	}
	if err := verifySnapshotRevision(clientKV, snapList[0]); err != nil {
		return err
	}

	// no more delta snapshots available
	if len(snapList) == 1 {
		return nil
	}

	var (
		remainingSnaps  = snapList[1:]
		numSnaps        = len(remainingSnaps)
		numFetchers     = int(math.Min(float64(numMaxFetchers), float64(numSnaps)))
		snapLocationsCh = make(chan string, numSnaps)
		errCh           = make(chan error, numFetchers+1)
		fetcherInfoCh   = make(chan types.FetcherInfo, numSnaps)
		applierInfoCh   = make(chan types.ApplierInfo, numSnaps)
		stopCh          = make(chan bool)
		wg              sync.WaitGroup
	)

	go r.applySnaps(clientKV, remainingSnaps, applierInfoCh, errCh, stopCh, &wg)

	for f := 0; f < numFetchers; f++ {
		go r.fetchSnaps(f, fetcherInfoCh, applierInfoCh, snapLocationsCh, errCh, stopCh, &wg)
	}

	for i, snap := range remainingSnaps {
		fetcherInfo := types.FetcherInfo{
			Snapshot:  *snap,
			SnapIndex: i,
		}
		fetcherInfoCh <- fetcherInfo
	}
	close(fetcherInfoCh)

	err := <-errCh
	r.cleanup(snapLocationsCh, stopCh, &wg)
	if err == nil {
		r.logger.Infof("Restoration complete.")
	} else {
		r.logger.Errorf("Restoration failed.")
	}

	return err
}

// applySnaps applies delta snapshot events to the embedded etcd sequentially, in the right order of snapshots, regardless of the order in which they were fetched.
func (r *Restorer) applySnaps(clientKV client.KVCloser, remainingSnaps brtypes.SnapList, applierInfoCh <-chan brtypes.ApplierInfo, errCh chan<- error, stopCh <-chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)

	pathList := make([]string, len(remainingSnaps))
	nextSnapIndexToApply := 0

	for {
		select {
		case _, more := <-stopCh:
			if !more {
				return
			}
		case applierInfo := <-applierInfoCh:
			if applierInfo.SnapIndex == -1 {
				return
			}

			fetchedSnapIndex := applierInfo.SnapIndex
			pathList[fetchedSnapIndex] = applierInfo.EventsFilePath

			if fetchedSnapIndex < nextSnapIndexToApply {
				errCh <- fmt.Errorf("snap index mismatch for delta snapshot %d; expected snap index to be atleast %d", fetchedSnapIndex, nextSnapIndexToApply)
				return
			}
			if fetchedSnapIndex == nextSnapIndexToApply {
				for currSnapIndex := fetchedSnapIndex; currSnapIndex < len(remainingSnaps); currSnapIndex++ {
					if pathList[currSnapIndex] == "" {
						break
					}

					r.logger.Infof("Applying delta snapshot %s", path.Join(remainingSnaps[currSnapIndex].SnapDir, remainingSnaps[currSnapIndex].SnapName))

					filePath := pathList[currSnapIndex]
					snapName := remainingSnaps[currSnapIndex].SnapName

					eventsData, err := os.ReadFile(filePath)
					if err != nil {
						errCh <- fmt.Errorf("failed to read events data from file for delta snapshot %s : %v", snapName, err)
						return
					}
					if err = os.Remove(filePath); err != nil {
						r.logger.Warnf("Unable to remove file: %s; err: %v", filePath, err)
					}
					events := []brtypes.Event{}
					if err = json.Unmarshal(eventsData, &events); err != nil {
						errCh <- fmt.Errorf("failed to read events from events data for delta snapshot %s : %v", snapName, err)
						return
					}

					if err := applyEventsAndVerify(clientKV, events, remainingSnaps[currSnapIndex]); err != nil {
						errCh <- err
						return
					}
					nextSnapIndexToApply++
					if nextSnapIndexToApply == len(remainingSnaps) {
						errCh <- nil // restore finished
						return
					}
				}
			}
		}
	}
}

// fetchSnaps fetches delta snapshots as events and persists them onto disk.
func (r *Restorer) fetchSnaps(fetcherIndex int, fetcherInfoCh <-chan brtypes.FetcherInfo, applierInfoCh chan<- brtypes.ApplierInfo, snapLocationsCh chan<- string, errCh chan<- error, stopCh chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)

	for fetcherInfo := range fetcherInfoCh {
		select {
		case _, more := <-stopCh:
			if !more {
				return
			}
		default:
			r.logger.Infof("Fetcher #%d fetching delta snapshot %s", fetcherIndex+1, path.Join(fetcherInfo.Snapshot.SnapDir, fetcherInfo.Snapshot.SnapName))

			eventsData, err := r.getEventsDataFromDeltaSnapshot(fetcherInfo.Snapshot)
			if err != nil {
				errCh <- fmt.Errorf("failed to read events data from delta snapshot %s : %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- brtypes.ApplierInfo{SnapIndex: -1} // cannot use close(ch) as concurrent fetchSnaps routines might try to send on channel, causing a panic
				return
			}

			eventsFilePath, err := persistDeltaSnapshot(eventsData)
			if err != nil {
				errCh <- fmt.Errorf("failed to persist events data for delta snapshot %s : %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- brtypes.ApplierInfo{SnapIndex: -1}
				return
			}

			snapLocationsCh <- eventsFilePath // used for cleanup later

			applierInfo := brtypes.ApplierInfo{
				EventsFilePath: eventsFilePath,
				SnapIndex:      fetcherInfo.SnapIndex,
			}
			applierInfoCh <- applierInfo
		}
	}
}
