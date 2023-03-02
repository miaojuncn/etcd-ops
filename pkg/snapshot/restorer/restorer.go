package restorer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	etypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

const (
	DefaultListenPeerURLs = "http://localhost:0"
	DefaultInitialAdvertisePeerURLs
	DefaultListenClientURLs
	DefaultAdvertiseClientURLs
	tmpDir                  = "/tmp"
	tmpEventsDataFilePrefix = "etcd-restore-"
)

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
	NewClientFactory etcd.NewClientFactoryFunc
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
func (r *Restorer) RestoreAndStopEtcd() error {
	embeddedEtcd, err := r.Restore()
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
	lpUrl, _ := url.Parse(DefaultListenPeerURLs)
	apUrl, _ := url.Parse(DefaultInitialAdvertisePeerURLs)
	lcUrl, _ := url.Parse(DefaultListenClientURLs)
	acUrl, _ := url.Parse(DefaultAdvertiseClientURLs)
	cfg.LPUrls = []url.URL{*lpUrl}
	cfg.LCUrls = []url.URL{*lcUrl}
	cfg.APUrls = []url.URL{*apUrl}
	cfg.ACUrls = []url.URL{*acUrl}
	// cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.InitialCluster = ro.Config.InitialCluster
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
		// trigger a shutdown
		e.Server.Stop()
		e.Close()
		return nil, fmt.Errorf("server took too long to start")
	}
	return e, nil
}

// Restore the etcd data directory as per specified restore options but returns the ETCD server that it started.
func (r *Restorer) Restore() (*embed.Etcd, error) {
	if err := r.restoreFromBaseSnapshot(); err != nil {
		return nil, fmt.Errorf("failed to restore from the base snapshot :%v", err)
	}
	if len(r.DeltaSnapList) == 0 {
		zap.S().Info("No delta snapshots present over base snapshot.")
		return nil, nil
	}
	zap.S().Info("Starting embedded etcd server...")
	e, err := StartEmbeddedEtcd(r)
	if err != nil {
		return e, err
	}

	clientFactory := etcd.NewClientFactory(r.NewClientFactory, types.EtcdConnectionConfig{
		MaxCallSendMsgSize: r.Config.MaxCallSendMsgSize,
		Endpoints:          []string{e.Clients[0].Addr().String()},
		InsecureTransport:  true,
	})
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return e, err
	}
	defer clientKV.Close()

	zap.S().Info("Applying delta snapshots...")
	if err := r.applyDeltaSnapshots(clientKV); err != nil {
		return e, err
	}

	return e, nil
}

// restoreFromBaseSnapshot restore the etcd data directory from base snapshot.
func (r *Restorer) restoreFromBaseSnapshot() error {
	var err error
	if path.Join(r.BaseSnapshot.SnapDir, r.BaseSnapshot.SnapName) == "" {
		zap.S().Warn("Base snapshot path not provided. Will do nothing.")
		return nil
	}
	zap.S().Infof("Restoring from base snapshot: %s", path.Join(r.BaseSnapshot.SnapDir, r.BaseSnapshot.SnapName))

	cfg := config.ServerConfig{
		InitialClusterToken: r.Config.InitialClusterToken,
		InitialPeerURLsMap:  r.ClusterURLs,
		PeerURLs:            r.PeerURLs,
		Name:                r.Config.Name,
	}
	if err := cfg.VerifyBootstrap(); err != nil {
		return err
	}
	cl, err := membership.NewClusterFromURLsMap(zap.S().Desugar(), r.Config.InitialClusterToken, r.ClusterURLs)
	if err != nil {
		return err
	}

	memberDir := filepath.Join(r.Config.RestoreDataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		return fmt.Errorf("member directory in data directory(%q) exists", memberDir)
	}

	walDir := filepath.Join(memberDir, "wal")
	snapDir := filepath.Join(memberDir, "snap")

	if err = r.makeDB(snapDir); err != nil {
		return err
	}
	hardState, err := makeWALAndSnap(walDir, snapDir, cl, r.Config.Name)
	if err != nil {
		return err
	}
	return updateCIndex(hardState.Commit, hardState.Term, snapDir)
}

func updateCIndex(commit uint64, term uint64, snapDir string) error {
	be := backend.NewDefaultBackend(filepath.Join(snapDir, "db"))
	defer be.Close()

	cindex.UpdateConsistentIndex(be.BatchTx(), commit, term)
	return nil
}

func makeWALAndSnap(walDir, snapDir string, cl *membership.RaftCluster, restoreName string) (*raftpb.HardState, error) {
	if err := fileutil.CreateDirAll(zap.S().Desugar(), walDir); err != nil {
		return nil, err
	}

	// add members again to persist them to the store we create.
	st := v2store.New(etcdserver.StoreClusterPrefix, etcdserver.StoreKeysPrefix)

	cl.SetStore(st)
	for _, m := range cl.Members() {
		cl.AddMember(m, true)
	}

	m := cl.MemberByName(restoreName)
	md := &etcdserverpb.Metadata{NodeID: uint64(m.ID), ClusterID: uint64(cl.ID())}
	metadata, err := md.Marshal()
	if err != nil {
		return nil, err
	}

	w, err := wal.Create(zap.S().Desugar(), walDir, metadata)
	if err != nil {
		return nil, err
	}
	defer w.Close()

	peers := make([]raft.Peer, len(cl.MemberIDs()))
	for i, id := range cl.MemberIDs() {
		ctx, err := json.Marshal((*cl).Member(id))
		if err != nil {
			return nil, err
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}

	ents := make([]raftpb.Entry, len(peers))
	nodeIDs := make([]uint64, len(peers))
	for i, p := range peers {
		nodeIDs[i] = p.ID
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  p.ID,
			Context: p.Context,
		}
		d, err := cc.Marshal()
		if err != nil {
			return nil, err
		}
		ents[i] = raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Term:  1,
			Index: uint64(i + 1),
			Data:  d,
		}
	}

	commit, term := uint64(len(ents)), uint64(1)
	hardState := raftpb.HardState{
		Term:   term,
		Vote:   peers[0].ID,
		Commit: commit}
	if err := w.Save(hardState, ents); err != nil {
		return nil, err
	}

	b, err := st.Save()
	if err != nil {
		return nil, err
	}

	raftSnap := raftpb.Snapshot{
		Data: b,
		Metadata: raftpb.SnapshotMetadata{
			Index: commit,
			Term:  term,
			ConfState: raftpb.ConfState{
				Voters: nodeIDs,
			},
		},
	}
	sn := snap.New(zap.S().Desugar(), snapDir)
	if err := sn.SaveSnap(raftSnap); err != nil {
		panic(err)
	}

	return &hardState, w.SaveSnapshot(walpb.Snapshot{Index: commit, Term: term})
}

// makeDB copies the database snapshot to the snapshot directory.
func (r *Restorer) makeDB(snapDir string) error {
	rc, err := r.Store.Fetch(*r.BaseSnapshot)
	if err != nil {
		return err
	}

	startTime := time.Now()
	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(r.BaseSnapshot.CompressionSuffix)
	if err != nil {
		return err
	}
	if isCompressed {
		// decompress the snapshot
		rc, err = compressor.DecompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return fmt.Errorf("unable to decompress the snapshot: %v", err)
		}
	}
	defer rc.Close()

	// create snap dir
	if err := fileutil.CreateDirAll(zap.S().Desugar(), snapDir); err != nil {
		return err
	}

	dbPath := filepath.Join(snapDir, "db")
	db, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(db, rc); err != nil {
		return err
	}
	db.Sync()
	totalTime := time.Now().Sub(startTime).Seconds()

	if isCompressed {
		zap.S().Infof("successfully fetched data of base snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		zap.S().Infof("successfully fetched data of base snapshot in %v seconds", totalTime)
	}

	off, err := db.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	hasHash := (off % 512) == sha256.Size
	if !hasHash && !r.Config.SkipHashCheck {
		err := fmt.Errorf("snapshot missing hash but --skip-hash-check=false")
		return err
	}

	if hasHash {
		// get snapshot integrity hash
		if _, err = db.Seek(-sha256.Size, io.SeekEnd); err != nil {
			return err
		}
		sha := make([]byte, sha256.Size)
		if _, err := db.Read(sha); err != nil {
			return fmt.Errorf("failed to read sha from db %v", err)
		}

		// truncate away integrity hash
		if err = db.Truncate(off - sha256.Size); err != nil {
			return err
		}

		if !r.Config.SkipHashCheck {
			if _, err := db.Seek(0, io.SeekStart); err != nil {
				return err
			}
			// check for match
			h := sha256.New()
			if _, err = io.Copy(h, db); err != nil {
				return err
			}
			dbSha := h.Sum(nil)
			if !reflect.DeepEqual(sha, dbSha) {
				err := fmt.Errorf("expected sha256 %v, got %v", sha, dbSha)
				return err
			}
		}
	}

	// db hash is OK
	db.Close()
	// update consistentIndex so applies go through on etcd server despite having a new raft instance
	be := backend.NewDefaultBackend(dbPath)
	defer be.Close()

	err = membership.TrimMembershipFromBackend(zap.S().Desugar(), be)
	if err != nil {
		return err
	}
	return nil
}

// applyDeltaSnapshots fetches the events from delta snapshots in parallel and applies them to the embedded etcd sequentially.
func (r *Restorer) applyDeltaSnapshots(clientKV client.KVCloser) error {
	snapList := r.DeltaSnapList
	numMaxFetchers := r.Config.MaxFetchers

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
		zap.S().Info("Restore complete.")
	} else {
		zap.S().Errorf("Restore failed.")
	}

	return err
}

// cleanup stops all running goroutines and removes the persisted snapshot files from disk.
func (r *Restorer) cleanup(snapLocationsCh chan string, stopCh chan bool, wg *sync.WaitGroup) {
	close(stopCh)

	wg.Wait()

	close(snapLocationsCh)

	for filePath := range snapLocationsCh {
		if _, err := os.Stat(filePath); err == nil && !os.IsNotExist(err) {
			if err = os.Remove(filePath); err != nil {
				zap.S().Warnf("Unable to remove file, file: %s, err: %v", filePath, err)
			}
		}
	}
	zap.S().Info("Cleanup complete")
}

// applySnaps applies delta snapshot events to the embedded etcd sequentially, in the right order of snapshots, regardless of the order in which they were fetched.
func (r *Restorer) applySnaps(clientKV client.KVCloser, remainingSnaps types.SnapList, applierInfoCh <-chan types.ApplierInfo, errCh chan<- error, stopCh <-chan bool, wg *sync.WaitGroup) {
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
				errCh <- fmt.Errorf("snap index mismatch for delta snapshot %d; expected snap index to be at least %d", fetchedSnapIndex, nextSnapIndexToApply)
				return
			}
			if fetchedSnapIndex == nextSnapIndexToApply {
				for currSnapIndex := fetchedSnapIndex; currSnapIndex < len(remainingSnaps); currSnapIndex++ {
					if pathList[currSnapIndex] == "" {
						break
					}

					zap.S().Infof("Applying delta snapshot %s", path.Join(remainingSnaps[currSnapIndex].SnapDir, remainingSnaps[currSnapIndex].SnapName))

					filePath := pathList[currSnapIndex]
					snapName := remainingSnaps[currSnapIndex].SnapName

					eventsData, err := os.ReadFile(filePath)
					if err != nil {
						errCh <- fmt.Errorf("failed to read events data from file for delta snapshot %s : %v", snapName, err)
						return
					}
					if err = os.Remove(filePath); err != nil {
						zap.S().Warnf("Unable to remove file: %s; err: %v", filePath, err)
					}
					var events []types.Event
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
func (r *Restorer) fetchSnaps(fetcherIndex int, fetcherInfoCh <-chan types.FetcherInfo, applierInfoCh chan<- types.ApplierInfo, snapLocationsCh chan<- string, errCh chan<- error, stopCh chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)

	for fetcherInfo := range fetcherInfoCh {
		select {
		case _, more := <-stopCh:
			if !more {
				return
			}
		default:
			zap.S().Infof("Fetcher #%d fetching delta snapshot %s", fetcherIndex+1, path.Join(fetcherInfo.Snapshot.SnapDir, fetcherInfo.Snapshot.SnapName))

			eventsData, err := r.getEventsDataFromDeltaSnapshot(fetcherInfo.Snapshot)
			if err != nil {
				errCh <- fmt.Errorf("failed to read events data from delta snapshot %s : %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- types.ApplierInfo{SnapIndex: -1} // cannot use close(ch) as concurrent fetchSnaps routines might try to send on channel, causing a panic
				return
			}

			eventsFilePath, err := persistDeltaSnapshot(eventsData)
			if err != nil {
				errCh <- fmt.Errorf("failed to persist events data for delta snapshot %s : %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- types.ApplierInfo{SnapIndex: -1}
				return
			}

			snapLocationsCh <- eventsFilePath // used for cleanup later

			applierInfo := types.ApplierInfo{
				EventsFilePath: eventsFilePath,
				SnapIndex:      fetcherInfo.SnapIndex,
			}
			applierInfoCh <- applierInfo
		}
	}
}

// applyFirstDeltaSnapshot applies the events from first delta snapshot to etcd.
func (r *Restorer) applyFirstDeltaSnapshot(clientKV client.KVCloser, snap types.Snapshot) error {
	zap.S().Infof("Applying first delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))
	events, err := r.getEventsFromDeltaSnapshot(snap)
	if err != nil {
		return fmt.Errorf("failed to read events from delta snapshot %s : %v", snap.SnapName, err)
	}

	// Note: Since revision in full snapshot file name might be lower than actual revision stored in snapshot.
	// This is because of issue refereed below. So, as per workaround used in our logic of taking delta snapshot,
	// latest revision from full snapshot may overlap with first few revision on first delta snapshot
	// Hence, we have to additionally take care of that.
	ctx := context.TODO()
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		return fmt.Errorf("failed to get etcd latest revision: %v", err)
	}
	lastRevision := resp.Header.Revision

	var newRevisionIndex int
	for index, event := range events {
		if event.EtcdEvent.Kv.ModRevision > lastRevision {
			newRevisionIndex = index
			break
		}
	}

	return applyEventsToEtcd(clientKV, events[newRevisionIndex:])
}

// getEventsFromDeltaSnapshot returns the events from delta snapshot from snap store.
func (r *Restorer) getEventsFromDeltaSnapshot(snap types.Snapshot) ([]types.Event, error) {
	data, err := r.getEventsDataFromDeltaSnapshot(snap)
	if err != nil {
		return nil, err
	}

	var events []types.Event
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}

	return events, nil
}

// getEventsDataFromDeltaSnapshot fetches the events data from delta snapshot from snap store.
func (r *Restorer) getEventsDataFromDeltaSnapshot(snap types.Snapshot) ([]byte, error) {
	rc, err := r.Store.Fetch(snap)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(snap.CompressionSuffix)
	if err != nil {
		return nil, err
	}
	if isCompressed {
		// decompress the snapshot
		rc, err = compressor.DecompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to decompress the snapshot: %v", err)
		}
	}
	defer rc.Close()

	buf := new(bytes.Buffer)
	bufSize, err := buf.ReadFrom(rc)
	if err != nil {
		return nil, err
	}
	totalTime := time.Now().Sub(startTime).Seconds()

	if isCompressed {
		zap.S().Infof("successfully fetched data of delta snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		zap.S().Infof("successfully fetched data of delta snapshot in %v seconds", totalTime)
	}
	sha := buf.Bytes()

	if bufSize <= sha256.Size {
		return nil, fmt.Errorf("delta snapshot is missing hash")
	}
	data := sha[:bufSize-sha256.Size]
	snapHash := sha[bufSize-sha256.Size:]

	// check for match
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, err
	}

	computedSha := h.Sum(nil)
	if !reflect.DeepEqual(snapHash, computedSha) {
		return nil, fmt.Errorf("expected sha256 %v, got %v", snapHash, computedSha)
	}

	return data, nil
}

// applyEventsAndVerify applies events from one snapshot to the embedded etcd and verifies the correctness of the sequence of snapshot applied.
func applyEventsAndVerify(clientKV client.KVCloser, events []types.Event, snap *types.Snapshot) error {
	if err := applyEventsToEtcd(clientKV, events); err != nil {
		return fmt.Errorf("failed to apply events to etcd for delta snapshot %s : %v", snap.SnapName, err)
	}

	if err := verifySnapshotRevision(clientKV, snap); err != nil {
		return fmt.Errorf("snapshot revision verification failed for delta snapshot %s : %v", snap.SnapName, err)
	}
	return nil
}

// applyEventsToEtcd performs operations in events sequentially.
func applyEventsToEtcd(clientKV client.KVCloser, events []types.Event) error {
	var (
		lastRev int64
		ops     []clientv3.Op
		ctx     = context.TODO()
	)

	for _, e := range events {
		ev := e.EtcdEvent
		nextRev := ev.Kv.ModRevision
		if lastRev != 0 && nextRev > lastRev {
			if _, err := clientKV.Txn(ctx).Then(ops...).Commit(); err != nil {
				return err
			}
			ops = []clientv3.Op{}
		}
		lastRev = nextRev
		switch ev.Type {
		case mvccpb.PUT:
			ops = append(ops, clientv3.OpPut(string(ev.Kv.Key), string(ev.Kv.Value))) // , clientv3.WithLease(clientv3.LeaseID(ev.Kv.Lease))))

		case mvccpb.DELETE:
			ops = append(ops, clientv3.OpDelete(string(ev.Kv.Key)))
		default:
			return fmt.Errorf("unexpected event type")
		}
	}
	_, err := clientKV.Txn(ctx).Then(ops...).Commit()
	return err
}

func verifySnapshotRevision(clientKV client.KVCloser, snap *types.Snapshot) error {
	ctx := context.TODO()
	getResponse, err := clientKV.Get(ctx, "foo")
	if err != nil {
		return fmt.Errorf("failed to connect to etcd KV client: %v", err)
	}
	etcdRevision := getResponse.Header.GetRevision()
	if snap.LastRevision != etcdRevision {
		return fmt.Errorf("mismatched event revision while applying delta snapshot, expected %d but applied %d ", snap.LastRevision, etcdRevision)
	}
	return nil
}

// persistDeltaSnapshot writes delta snapshot events to disk and returns the file path for the same.
func persistDeltaSnapshot(data []byte) (string, error) {
	tmpFile, err := os.CreateTemp(tmpDir, tmpEventsDataFilePrefix)
	if err != nil {
		err = fmt.Errorf("failed to create temp file")
		return "", err
	}
	defer tmpFile.Close()

	if _, err = tmpFile.Write(data); err != nil {
		err = fmt.Errorf("failed to write events data into temp file")
		return "", err
	}

	return tmpFile.Name(), nil
}
