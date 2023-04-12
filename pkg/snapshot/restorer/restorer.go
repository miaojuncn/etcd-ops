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
	"strings"
	"sync"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
)

const (
	DefaultListenPeerURLs = "http://localhost:0"
	DefaultInitialAdvertisePeerURLs
	DefaultListenClientURLs
	DefaultAdvertiseClientURLs
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

	zlog.Logger.Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := tools.GetLatestFullSnapshotAndDeltaSnapList(s)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %v", err)
	}

	if baseSnap == nil {
		zlog.Logger.Info("No base snapshot found, will do nothing.")
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
	cfg.Dir = filepath.Join(ro.Config.DataDir)
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
		zlog.Logger.Infof("Embedded server is ready to listen client at: %s", e.Clients[0].Addr())
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
		return nil, fmt.Errorf("failed to restore from the base snapshot: %v", err)
	}
	if len(r.DeltaSnapList) == 0 {
		zlog.Logger.Info("No delta snapshots present over base snapshot.")
		return nil, nil
	}

	zlog.Logger.Infof("Attempting to apply %d delta snapshots for restore", len(r.DeltaSnapList))
	zlog.Logger.Infof("Creating tempoary directory %s for persisting delta snapshots locally", r.Config.TempSnapshotsDir)
	err := os.MkdirAll(r.Config.TempSnapshotsDir, 0700)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = os.RemoveAll(r.Config.TempSnapshotsDir); err != nil {
			zlog.Logger.Errorf("Failed to remove restore temp directory %s: %v", r.Config.TempSnapshotsDir, err)
		}
	}()

	zlog.Logger.Info("Starting embedded etcd server...")
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
	defer func() {
		if err = clientKV.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close etcd KV client: %v", err)
		}
	}()

	zlog.Logger.Info("Applying delta snapshots...")
	if err = r.applyDeltaSnapshots(clientKV); err != nil {
		return e, err
	}

	return e, nil
}

// restoreFromBaseSnapshot restore the etcd data directory from base snapshot.
func (r *Restorer) restoreFromBaseSnapshot() error {
	if path.Join(r.BaseSnapshot.SnapDir, r.BaseSnapshot.SnapName) == "" {
		zlog.Logger.Warn("Base snapshot path not provided. Will do nothing.")
		return nil
	}
	zlog.Logger.Infof("Restoring from base snapshot: %s", path.Join(r.BaseSnapshot.SnapDir, r.BaseSnapshot.SnapName))

	cfg := config.ServerConfig{
		InitialClusterToken: r.Config.InitialClusterToken,
		InitialPeerURLsMap:  r.ClusterURLs,
		PeerURLs:            r.PeerURLs,
		Name:                r.Config.Name,
	}
	if err := cfg.VerifyBootstrap(); err != nil {
		return err
	}
	cl, err := membership.NewClusterFromURLsMap(zlog.Logger.Desugar(), r.Config.InitialClusterToken, r.ClusterURLs)
	if err != nil {
		return err
	}

	memberDir := filepath.Join(r.Config.DataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		return fmt.Errorf("member directory in data directory(%q) exists", memberDir)
	}

	walDir := filepath.Join(memberDir, "wal")
	snapDir := filepath.Join(memberDir, "snap")
	// clean up the raft meta information in the backup file
	if err = r.saveDB(snapDir); err != nil {
		return err
	}
	// restore the backup file to the wal and snap files required for the raft startup
	hardState, err := saveWALAndSnap(walDir, snapDir, cl, r.Config.Name)
	if err != nil {
		return err
	}
	// update index information to boltdb
	return updateCIndex(hardState.Commit, hardState.Term, snapDir)
}

func updateCIndex(commit uint64, term uint64, snapDir string) error {
	be := backend.NewDefaultBackend(filepath.Join(snapDir, "db"))
	defer func() {
		if err := be.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close etcd default backend: %v", err)
		}
	}()

	cindex.UpdateConsistentIndex(be.BatchTx(), commit, term)
	return nil
}

// saveDB copies the database snapshot to the snapshot directory.
func (r *Restorer) saveDB(snapDir string) error {
	rc, err := r.Store.Fetch(*r.BaseSnapshot)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close store fetcher: %v", err)
		}
	}()

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

	// create snap dir
	if err = os.MkdirAll(snapDir, 0700); err != nil {
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

	if err := db.Sync(); err != nil {
		return err
	}

	totalTime := time.Since(startTime).Seconds()

	if isCompressed {
		zlog.Logger.Infof("successfully fetched data of base snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		zlog.Logger.Infof("successfully fetched data of base snapshot in %v seconds", totalTime)
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
	if err = db.Close(); err != nil {
		return err
	}

	be := backend.NewDefaultBackend(dbPath)
	defer func() {
		if err := be.Close(); err != nil {
			zlog.Logger.Warnf("Failed to close etcd default backend: %v", err)
		}
	}()
	// delete the raft meta information in the backup data
	err = membership.TrimMembershipFromBackend(zlog.Logger.Desugar(), be)
	if err != nil {
		return err
	}
	return nil
}

// saveWALAndSnap creates a WAL for the initial cluster
func saveWALAndSnap(walDir, snapDir string, cl *membership.RaftCluster, restoreName string) (*raftpb.HardState, error) {
	if err := os.MkdirAll(walDir, 0700); err != nil {
		return nil, err
	}

	// add members again to persist them to the store we create.
	st := v2store.New(etcdserver.StoreClusterPrefix, etcdserver.StoreKeysPrefix)
	cl.SetStore(st)
	be := backend.NewDefaultBackend(filepath.Join(snapDir, "db"))
	defer func() {
		if err := be.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close etcd default backend: %v", err)
		}
	}()
	cl.SetBackend(be)
	// write raft information to boltdb
	for _, m := range cl.Members() {
		cl.AddMember(m, true)
	}
	// initialize the cluster's meta information, nodeID and clusterID, create a wal file, and write the meta information
	m := cl.MemberByName(restoreName)
	md := &etcdserverpb.Metadata{NodeID: uint64(m.ID), ClusterID: uint64(cl.ID())}
	metadata, err := md.Marshal()
	if err != nil {
		return nil, err
	}

	w, err := wal.Create(zlog.Logger.Desugar(), walDir, metadata)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := w.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close wal when creating wal file: %v", err)
		}
	}()

	peers := make([]raft.Peer, len(cl.MemberIDs()))
	for i, id := range cl.MemberIDs() {
		ctx, err := json.Marshal((*cl).Member(id))
		if err != nil {
			return nil, err
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	// initialize the configuration change log for each node
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
	// initialize the term and log submission information of raft and save it to hardState
	commit, term := uint64(len(ents)), uint64(1)
	hardState := raftpb.HardState{
		Term:   term,
		Vote:   peers[0].ID,
		Commit: commit}
	// persisting logs and hardState to wal
	if err := w.Save(hardState, ents); err != nil {
		return nil, err
	}
	// create a raft snapshot for the current state machine (recovered data) and write the corresponding snapshot information to the wal log
	b, err := st.Save()
	if err != nil {
		return nil, err
	}

	confState := raftpb.ConfState{
		Voters: nodeIDs,
	}
	raftSnap := raftpb.Snapshot{
		Data: b,
		Metadata: raftpb.SnapshotMetadata{
			Index:     commit,
			Term:      term,
			ConfState: confState,
		},
	}
	sn := snap.New(zlog.Logger.Desugar(), snapDir)
	if err := sn.SaveSnap(raftSnap); err != nil {
		panic(err)
	}

	return &hardState, w.SaveSnapshot(walpb.Snapshot{Index: commit, Term: term, ConfState: &confState})
}

// applyDeltaSnapshots fetches the events from delta snapshots in parallel and applies them to the embedded etcd sequentially.
func (r *Restorer) applyDeltaSnapshots(clientKV client.KVCloser) error {
	snapList := r.DeltaSnapList
	numMaxFetchers := r.Config.MaxFetchers

	firstDeltaSnap := snapList[0]

	if err := r.applyFirstDeltaSnapshot(clientKV, firstDeltaSnap); err != nil {
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
		go r.fetchSnaps(f, fetcherInfoCh, applierInfoCh, snapLocationsCh, errCh, stopCh, &wg, r.Config.TempSnapshotsDir)
	}

	for i, remainingSnap := range remainingSnaps {
		fetcherInfo := types.FetcherInfo{
			Snapshot:  *remainingSnap,
			SnapIndex: i,
		}
		fetcherInfoCh <- fetcherInfo
	}
	close(fetcherInfoCh)

	err := <-errCh

	if cleanupErr := r.cleanup(snapLocationsCh, stopCh, &wg); cleanupErr != nil {
		zlog.Logger.Errorf("Cleanup of temporary snapshots failed: %v", cleanupErr)
	}

	if err != nil {
		zlog.Logger.Info("Restore failed.")
		return err
	}

	zlog.Logger.Info("Restore complete.")
	return nil
}

// cleanup stops all running goroutines and removes the persisted snapshot files from disk.
func (r *Restorer) cleanup(snapLocationsCh chan string, stopCh chan bool, wg *sync.WaitGroup) error {
	var errs []error

	close(stopCh)
	wg.Wait()
	close(snapLocationsCh)

	for filePath := range snapLocationsCh {
		if _, err := os.Stat(filePath); err == nil {
			if !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("unable to stat file %s: %v", filePath, err))
			}
			continue
		}
		if err := os.Remove(filePath); err != nil {
			errs = append(errs, fmt.Errorf("unable to remove file %s: %v", filePath, err))
		}
	}

	if len(errs) != 0 {
		zlog.Logger.Error("Cleanup failed")
		return ErrorArrayToError(errs)
	}
	zlog.Logger.Info("Cleanup complete")
	return nil
}

// ErrorArrayToError takes an array of errors and returns a single concatenated error
func ErrorArrayToError(errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	var errString string

	for _, e := range errs {
		errString = fmt.Sprintf("%s\n%s", errString, e.Error())
	}

	return fmt.Errorf("%s", strings.TrimSpace(errString))
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

					filePath := pathList[currSnapIndex]
					snapName := remainingSnaps[currSnapIndex].SnapName

					zlog.Logger.Infof("Reading snapshot contents %s from raw snapshot file %s", snapName, filePath)

					eventsData, err := r.readSnapshotContentsFromFile(filePath, remainingSnaps[currSnapIndex])
					if err != nil {
						errCh <- fmt.Errorf("failed to read events data from file for delta snapshot %s : %v", snapName, err)
						return
					}

					var events []types.Event
					if err = json.Unmarshal(eventsData, &events); err != nil {
						errCh <- fmt.Errorf("failed to unmarshal events from events data for delta snapshot %s : %v", snapName, err)
						return
					}

					zlog.Logger.Infof("Applying delta snapshot %s [%d/%d]", path.Join(remainingSnaps[currSnapIndex].SnapDir, remainingSnaps[currSnapIndex].SnapName), currSnapIndex+2, len(remainingSnaps)+1)
					if err := applyEventsAndVerify(clientKV, events, remainingSnaps[currSnapIndex]); err != nil {
						errCh <- err
						return
					}

					zlog.Logger.Infof("Removing temporary delta snapshot events file %s for snapshot %s", filePath, snapName)
					if err = os.Remove(filePath); err != nil {
						zlog.Logger.Warnf("Unable to remove file: %s; err: %v", filePath, err)
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
func (r *Restorer) fetchSnaps(fetcherIndex int, fetcherInfoCh <-chan types.FetcherInfo,
	applierInfoCh chan<- types.ApplierInfo, snapLocationsCh chan<- string, errCh chan<- error, stopCh chan bool,
	wg *sync.WaitGroup, tempDir string) {
	defer wg.Done()
	wg.Add(1)

	for fetcherInfo := range fetcherInfoCh {
		select {
		case _, more := <-stopCh:
			if !more {
				return
			}
		default:
			zlog.Logger.Infof("Fetcher #%d fetching delta snapshot %s", fetcherIndex+1, path.Join(fetcherInfo.Snapshot.SnapDir, fetcherInfo.Snapshot.SnapName))

			rc, err := r.Store.Fetch(fetcherInfo.Snapshot)
			if err != nil {
				errCh <- fmt.Errorf("failed to fetch delta snapshot %s from store: %v", fetcherInfo.Snapshot.SnapName, err)
				applierInfoCh <- types.ApplierInfo{SnapIndex: -1} // cannot use close(ch) as concurrent fetchSnaps routines might try to send on channel, causing a panic
			}

			snapTempFilePath := filepath.Join(tempDir, fetcherInfo.Snapshot.SnapName)
			err = persistDeltaSnapshot(rc, snapTempFilePath)
			if err != nil {
				errCh <- fmt.Errorf("failed to persist delta snapshot %s to temp file path %s : %v", fetcherInfo.Snapshot.SnapName, snapTempFilePath, err)
				applierInfoCh <- types.ApplierInfo{SnapIndex: -1}
			}

			snapLocationsCh <- snapTempFilePath // used for cleanup later

			applierInfo := types.ApplierInfo{
				EventsFilePath: snapTempFilePath,
				SnapIndex:      fetcherInfo.SnapIndex,
			}
			applierInfoCh <- applierInfo
		}
	}
}

// applyFirstDeltaSnapshot applies the events from first delta snapshot to etcd.
func (r *Restorer) applyFirstDeltaSnapshot(clientKV client.KVCloser, snap *types.Snapshot) error {
	zlog.Logger.Infof("Applying first delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))

	rc, err := r.Store.Fetch(*snap)
	if err != nil {
		return fmt.Errorf("failed to fetch delta snapshot %s from store : %v", snap.SnapName, err)
	}

	eventsData, err := r.readSnapshotContentsFromReadCloser(rc, snap)
	if err != nil {
		return fmt.Errorf("failed to read events data from delta snapshot %s : %v", snap.SnapName, err)
	}

	var events []types.Event
	if err = json.Unmarshal(eventsData, &events); err != nil {
		return fmt.Errorf("failed to unmarshal events data from delta snapshot %s : %v", snap.SnapName, err)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), types.DefaultEtcdConnectionTimeout)
	defer cancel()
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
	zlog.Logger.Infof("Applying first delta snapshot %s", path.Join(snap.SnapDir, snap.SnapName))
	return applyEventsToEtcd(clientKV, events[newRevisionIndex:])
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

func persistDeltaSnapshot(rc io.ReadCloser, tempFilePath string) error {
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		err = fmt.Errorf("failed to create temp file %s to store raw delta snapshot", tempFilePath)
		return err
	}
	defer func() {
		_ = tempFile.Close()
	}()

	_, err = tempFile.ReadFrom(rc)
	if err != nil {
		return err
	}

	return rc.Close()
}

// getNormalizedSnapshotReadCloser passes the given ReadCloser through the
// snapshot decompressor if the snapshot is compressed using a compression policy.
// If snapshot is not compressed, it returns the given ReadCloser as is.
// It also returns whether the snapshot was initially compressed or not, as well as
// the compression policy used for compressing the snapshot.
func getNormalizedSnapshotReadCloser(rc io.ReadCloser, snap *types.Snapshot) (io.ReadCloser, bool, string, error) {
	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(snap.CompressionSuffix)
	if err != nil {
		return rc, false, "", err
	}

	if isCompressed {
		// decompress the snapshot
		rc, err = compressor.DecompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return rc, true, compressionPolicy, fmt.Errorf("unable to decompress the snapshot: %v", err)
		}
	}

	return rc, isCompressed, compressionPolicy, nil
}

func (r *Restorer) readSnapshotContentsFromReadCloser(rc io.ReadCloser, snap *types.Snapshot) ([]byte, error) {
	startTime := time.Now()

	rc, wasCompressed, compressionPolicy, err := getNormalizedSnapshotReadCloser(rc, snap)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress delta snapshot %s : %v", snap.SnapName, err)
	}

	buf := new(bytes.Buffer)
	bufSize, err := buf.ReadFrom(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to parse contents from delta snapshot %s : %v", snap.SnapName, err)
	}

	totalTime := time.Since(startTime).Seconds()
	if wasCompressed {
		zlog.Logger.Infof("successfully decompressed data of delta snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		zlog.Logger.Infof("successfully read the data of delta snapshot in %v seconds", totalTime)
	}

	if bufSize <= sha256.Size {
		return nil, fmt.Errorf("delta snapshot is missing hash")
	}

	sha := buf.Bytes()
	data := sha[:bufSize-sha256.Size]
	snapHash := sha[bufSize-sha256.Size:]

	// check for match
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, fmt.Errorf("unable to check integrity of snapshot %s: %v", snap.SnapName, err)
	}

	computedSha := h.Sum(nil)
	if !reflect.DeepEqual(snapHash, computedSha) {
		return nil, fmt.Errorf("expected sha256 %v, got %v", snapHash, computedSha)
	}

	return data, nil
}

func (r *Restorer) readSnapshotContentsFromFile(filePath string, snap *types.Snapshot) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s for delta snapshot %s : %v", filePath, snap.SnapName, err)
	}

	return r.readSnapshotContentsFromReadCloser(file, snap)
}

// DeepCopy returns a deeply copied structure.
func (r *Restorer) DeepCopy() *Restorer {
	if r == nil {
		return nil
	}

	out := new(Restorer)
	r.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the structure deeply from r to out.
func (r *Restorer) DeepCopyInto(out *Restorer) {
	*out = *r
	if r.Config != nil {
		in, out := &r.Config, &out.Config
		*out = new(types.RestoreConfig)
		(*in).DeepCopyInto(*out)
	}
	if r.ClusterURLs != nil {
		in, out := &r.ClusterURLs, &out.ClusterURLs
		*out = make(etypes.URLsMap)
		for k := range *in {
			if (*in)[k] != nil {
				(*out)[k] = DeepCopyURLs((*in)[k])
			}
		}
	}
	if r.PeerURLs != nil {
		out.PeerURLs = DeepCopyURLs(r.PeerURLs)
	}
	if r.DeltaSnapList != nil {
		out.DeltaSnapList = DeepCopySnapList(r.DeltaSnapList)
	}
	if r.NewClientFactory != nil {
		out.NewClientFactory = DeepCopyNewClientFactory(r.NewClientFactory)
	}
}

// DeepCopySnapList returns a deep copy
func DeepCopySnapList(in types.SnapList) types.SnapList {
	out := make(types.SnapList, len(in))
	for i, v := range in {
		if v != nil {
			var cpv = *v
			out[i] = &cpv
		}
	}
	return out
}

// DeepCopyNewClientFactory returns a deep copy
func DeepCopyNewClientFactory(in etcd.NewClientFactoryFunc) etcd.NewClientFactoryFunc {
	var out etcd.NewClientFactoryFunc
	out = in
	return out
}

// DeepCopyURLs returns a deep  copy
func DeepCopyURLs(in etypes.URLs) etypes.URLs {
	out := make(etypes.URLs, len(in))
	for i, u := range in {
		out[i] = *(DeepCopyURL(&u))
	}
	return out
}

// DeepCopyURL returns a deep copy
func DeepCopyURL(in *url.URL) *url.URL {
	var out = new(url.URL)
	*out = *in
	if in.User != nil {
		in, out := &in.User, &out.User
		*out = new(url.Userinfo)
		*out = *in
	}
	return out
}
