package snapaction

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/errors"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	emptyStruct   struct{}
	snapStoreHash = make(map[string]interface{})
)

type result struct {
	Snapshot *types.Snapshot `json:"snapshot"`
	Err      error           `json:"error"`
}

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

type SnapAction struct {
	etcdConnectionConfig *types.EtcdConnectionConfig
	store                types.Store
	policy               *types.SnapPolicyConfig
	compressionConfig    *types.CompressionConfig
	schedule             cron.Schedule
	prevSnapshot         *types.Snapshot
	PrevFullSnapshot     *types.Snapshot
	PrevDeltaSnapshots   types.SnapList
	fullSnapshotReqCh    chan bool
	deltaSnapshotReqCh   chan struct{}
	fullSnapshotAckCh    chan result
	deltaSnapshotAckCh   chan result
	fullSnapshotTimer    *time.Timer
	deltaSnapshotTimer   *time.Timer
	events               []byte
	watchCh              clientv3.WatchChan
	etcdWatchClient      *clientv3.Watcher
	cancelWatch          context.CancelFunc
	SnapActionMutex      *sync.Mutex
	SnapActionState      types.SnapActionState
	lastEventRevision    int64
	storeConfig          *types.StoreConfig
}

func NewSnapAction(etcdConnectionConfig *types.EtcdConnectionConfig, policy *types.SnapPolicyConfig,
	compressionConfig *types.CompressionConfig, storeConfig *types.StoreConfig, store types.Store) (*SnapAction, error) {

	sdl, err := cron.ParseStandard(policy.FullSnapshotSchedule)
	if err != nil {
		return nil, fmt.Errorf("invalid full snapshot schedule provided %s : %v", policy.FullSnapshotSchedule, err)
	}

	var prevSnapshot *types.Snapshot
	fullSnap, deltaSnapList, err := tools.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		return nil, err
	} else if fullSnap != nil && len(deltaSnapList) == 0 {
		prevSnapshot = fullSnap
	} else if fullSnap != nil && len(deltaSnapList) != 0 {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
	} else {
		prevSnapshot = types.NewSnapshot(types.SnapshotKindFull, 0, 0, "", false)
	}
	return &SnapAction{
		etcdConnectionConfig: etcdConnectionConfig,
		store:                store,
		policy:               policy,
		compressionConfig:    compressionConfig,
		schedule:             sdl,
		prevSnapshot:         prevSnapshot,
		PrevFullSnapshot:     fullSnap,
		PrevDeltaSnapshots:   deltaSnapList,
		fullSnapshotReqCh:    make(chan bool),
		deltaSnapshotReqCh:   make(chan struct{}),
		fullSnapshotAckCh:    make(chan result),
		deltaSnapshotAckCh:   make(chan result),

		cancelWatch:       func() {},
		SnapActionMutex:   &sync.Mutex{},
		SnapActionState:   types.SnapActionInactive,
		lastEventRevision: 0,
		storeConfig:       storeConfig,
	}, nil
}

func (sa *SnapAction) Run(stopCh <-chan struct{}, startWithFullSnapshot bool) error {
	defer sa.stop()
	if startWithFullSnapshot {
		sa.fullSnapshotTimer = time.NewTimer(0)
	} else {
		// for the case when snap action is run for the first time on
		// a fresh etcd with startWithFullSnapshot set to false, we need
		// to take the first delta snapshot(s) initially and then set
		// the full snapshot schedule
		if sa.watchCh == nil {
			ssrStopped, err := sa.CollectEventsSincePrevSnapshot(stopCh)
			if ssrStopped {
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if err := sa.resetFullSnapshotTimer(); err != nil {
			return fmt.Errorf("failed to reset full snapshot timer: %v", err)
		}
	}

	sa.deltaSnapshotTimer = time.NewTimer(types.DefaultDeltaSnapshotInterval)
	if sa.policy.DeltaSnapshotPeriod >= types.DeltaSnapshotIntervalThreshold {
		sa.deltaSnapshotTimer.Stop()
		sa.deltaSnapshotTimer.Reset(sa.policy.DeltaSnapshotPeriod)
	}

	return sa.snapshotEventHandler(stopCh)
}

// TriggerFullSnapshot sends the events to take full snapshot. This is to trigger full snapshot externally out of regular schedule.
func (sa *SnapAction) TriggerFullSnapshot(ctx context.Context, isFinal bool) (*types.Snapshot, error) {
	sa.SnapActionMutex.Lock()
	defer sa.SnapActionMutex.Unlock()

	if sa.SnapActionState != types.SnapActionActive {
		return nil, fmt.Errorf("snaptaker is not active")
	}
	zap.S().Info("Triggering out of schedule full snapshot...")
	sa.fullSnapshotReqCh <- isFinal
	res := <-sa.fullSnapshotAckCh
	return res.Snapshot, res.Err
}

// TriggerDeltaSnapshot sends the events to take delta snapshot. This is to
// trigger delta snapshot externally out of regular schedule.
func (sa *SnapAction) TriggerDeltaSnapshot() (*types.Snapshot, error) {
	sa.SnapActionMutex.Lock()
	defer sa.SnapActionMutex.Unlock()

	if sa.SnapActionState != types.SnapActionActive {
		return nil, fmt.Errorf("snapshotter is not active")
	}
	if sa.policy.DeltaSnapshotPeriod < types.DeltaSnapshotIntervalThreshold {
		return nil, fmt.Errorf("found delta snapshot interval %s less than %v. Delta snapshotting is disabled. ", sa.policy.DeltaSnapshotPeriod, types.DeltaSnapshotIntervalThreshold)
	}
	zap.S().Info("Triggering out of schedule delta snapshot...")
	sa.deltaSnapshotReqCh <- emptyStruct
	res := <-sa.deltaSnapshotAckCh
	return res.Snapshot, res.Err
}

// stop stops the snapshot. Once stopped any subsequent calls will not have any effect.
func (sa *SnapAction) stop() {
	zap.S().Info("Closing the Snapshot")

	if sa.fullSnapshotTimer != nil {
		sa.fullSnapshotTimer.Stop()
		sa.fullSnapshotTimer = nil
	}
	if sa.deltaSnapshotTimer != nil {
		sa.deltaSnapshotTimer.Stop()
		sa.deltaSnapshotTimer = nil
	}
	sa.SetSnapActionInactive()
	sa.closeEtcdClient()
}

// SetSnapActionInactive set the snapshot state to Inactive.
func (sa *SnapAction) SetSnapActionInactive() {
	sa.SnapActionMutex.Lock()
	defer sa.SnapActionMutex.Unlock()
	sa.SnapActionState = types.SnapActionInactive
}

// SetSnapActionActive set the snapshot state to active.
func (sa *SnapAction) SetSnapActionActive() {
	sa.SnapActionMutex.Lock()
	defer sa.SnapActionMutex.Unlock()
	sa.SnapActionState = types.SnapActionActive
}

func (sa *SnapAction) closeEtcdClient() {
	if sa.cancelWatch != nil {
		sa.cancelWatch()
		sa.cancelWatch = nil
	}
	if sa.watchCh != nil {
		sa.watchCh = nil
	}

	if sa.etcdWatchClient != nil {
		if err := (*sa.etcdWatchClient).Close(); err != nil {
			zap.S().Warnf("Error while closing etcd watch client connection, %v", err)
		}
		sa.etcdWatchClient = nil
	}
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot timer as per the schedule.
func (sa *SnapAction) TakeFullSnapshotAndResetTimer(isFinal bool) (*types.Snapshot, error) {
	zap.S().Infof("Taking scheduled full snapshot for time: %s", time.Now().Local())
	s, err := sa.takeFullSnapshot(isFinal)
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		zap.S().Warnf("Taking scheduled full snapshot failed: %v", err)
		return nil, err
	}

	return s, sa.resetFullSnapshotTimer()
}

// takeFullSnapshot will store full snapshot of etcd.
// It basically will connect to etcd. Then ask for snapshot. And finally store it to underlying snapstore on the fly.
func (sa *SnapAction) takeFullSnapshot(isFinal bool) (*types.Snapshot, error) {
	defer sa.cleanupInMemoryEvents()
	// close previous watch and client.
	sa.closeEtcdClient()

	var err error

	// Update the snapstore object before taking every full snapshot
	sa.store, err = store.GetStore(sa.storeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
	}

	clientFactory := etcd.NewFactory(*sa.etcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), sa.etcdConnectionConfig.ConnectionTimeout)
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision

	if sa.prevSnapshot.Kind == types.SnapshotKindFull && sa.prevSnapshot.LastRevision == lastRevision && sa.prevSnapshot.IsFinal == isFinal {
		zap.S().Info("There are no updates since last snapshot, skipping full snapshot.")
	} else {
		// Note: As FullSnapshot size can be very large, so to avoid context timeout use "SnapshotTimeout" in context.WithTimeout()
		ctx, cancel = context.WithTimeout(context.TODO(), sa.etcdConnectionConfig.SnapshotTimeout)
		defer cancel()
		// compressionSuffix is useful in backward compatibility(restoring from uncompressed snapshots).
		// it is also helpful in inferring which compression Policy to be used to decompress the snapshot.
		compressionSuffix, err := compressor.GetCompressionSuffix(sa.compressionConfig.Enabled, sa.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
		}

		clientMaintenance, err := clientFactory.NewMaintenance()
		if err != nil {
			return nil, fmt.Errorf("failed to build etcd maintenance client")
		}
		defer clientMaintenance.Close()

		s, err := etcd.TakeAndSaveFullSnapshot(ctx, clientMaintenance, sa.store, lastRevision, sa.compressionConfig, compressionSuffix, isFinal)
		if err != nil {
			return nil, err
		}

		sa.prevSnapshot = s
		sa.PrevFullSnapshot = s
		sa.PrevDeltaSnapshots = nil

		zap.S().Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}

	if sa.policy.DeltaSnapshotPeriod < time.Second {
		// return without creating a watch on events
		return sa.prevSnapshot, nil
	}

	etcdWatchClient, err := clientFactory.NewWatcher()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd watch client for snapshot: %v", err),
		}
	}
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	sa.cancelWatch = cancelWatch
	sa.etcdWatchClient = &etcdWatchClient
	sa.watchCh = etcdWatchClient.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(sa.prevSnapshot.LastRevision+1))
	zap.S().Infof("Applied watch on etcd from revision: %d", sa.prevSnapshot.LastRevision+1)

	return sa.prevSnapshot, nil
}

func (sa *SnapAction) cleanupInMemoryEvents() {
	sa.events = []byte{}
	sa.lastEventRevision = -1
}

func (sa *SnapAction) takeDeltaSnapshotAndResetTimer() (*types.Snapshot, error) {
	s, err := sa.TakeDeltaSnapshot()
	if err != nil {
		zap.S().Infof("Taking delta snapshot failed: %v", err)
		return nil, err
	}

	if sa.deltaSnapshotTimer == nil {
		sa.deltaSnapshotTimer = time.NewTimer(sa.policy.DeltaSnapshotPeriod)
	} else {
		zap.S().Info("Stopping delta snapshot...")
		sa.deltaSnapshotTimer.Stop()
		zap.S().Infof("Resetting delta snapshot to run after %s.", sa.policy.DeltaSnapshotPeriod.String())
		sa.deltaSnapshotTimer.Reset(sa.policy.DeltaSnapshotPeriod)
	}
	return s, nil
}

// TakeDeltaSnapshot takes a delta snapshot that contains the etcd events collected up till now
func (sa *SnapAction) TakeDeltaSnapshot() (*types.Snapshot, error) {
	defer sa.cleanupInMemoryEvents()
	zap.S().Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(sa.events) == 0 {
		zap.S().Info("No events received to save snapshot. Skipping delta snapshot.")
		return nil, nil
	}
	sa.events = append(sa.events, byte(']'))

	isSecretUpdated := sa.checkStoreSecretUpdate()
	if isSecretUpdated {
		var err error

		// Update the snap store object before taking every delta snapshot
		sa.store, err = store.GetStore(sa.storeConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
		}
		zap.S().Info("updated the snapstore object with new credentials")
	}

	// compressionSuffix is useful in backward compatibility(restoring from uncompressed snapshots).
	// it is also helpful in inferring which compression Policy to be used to decompress the snapshot.
	compressionSuffix, err := compressor.GetCompressionSuffix(sa.compressionConfig.Enabled, sa.compressionConfig.CompressionPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
	}
	snap := types.NewSnapshot(types.SnapshotKindDelta, sa.prevSnapshot.LastRevision+1, sa.lastEventRevision, compressionSuffix, false)

	// compute hash
	hash := sha256.New()
	if _, err := hash.Write(sa.events); err != nil {
		return nil, fmt.Errorf("failed to compute hash of events: %v", err)
	}
	sa.events = hash.Sum(sa.events)

	startTime := time.Now()
	rc := io.NopCloser(bytes.NewReader(sa.events))

	// if compression is enabled then compress the snapshot.
	if sa.compressionConfig.Enabled {
		zap.S().Info("start the Compression of delta snapshot")
		rc, err = compressor.CompressSnapshot(rc, sa.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to compress delta snapshot: %v", err)
		}
	}
	defer rc.Close()

	if err := sa.store.Save(*snap, rc); err != nil {
		zap.S().Errorf("Error saving delta snapshots. %v", err)
		return nil, err
	}
	timeTaken := time.Since(startTime).Seconds()
	zap.S().Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	sa.prevSnapshot = snap
	sa.PrevDeltaSnapshots = append(sa.PrevDeltaSnapshots, snap)

	zap.S().Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return snap, nil
}

// CollectEventsSincePrevSnapshot takes the first delta snapshot on etcd startup.
func (sa *SnapAction) CollectEventsSincePrevSnapshot(stopCh <-chan struct{}) (bool, error) {
	// close any previous watch and client.
	sa.closeEtcdClient()

	clientFactory := etcd.NewFactory(*sa.etcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), sa.etcdConnectionConfig.ConnectionTimeout)
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastEtcdRevision := resp.Header.Revision

	etcdWatchClient, err := clientFactory.NewWatcher()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd watch client for snapshot: %v", err),
		}
	}
	// TODO: Use parent context. Passing parent context here directly requires some additional management of error handling.
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	sa.cancelWatch = cancelWatch
	sa.etcdWatchClient = &etcdWatchClient
	sa.watchCh = etcdWatchClient.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(sa.prevSnapshot.LastRevision+1))
	zap.S().Infof("Applied watch on etcd from revision: %d", sa.prevSnapshot.LastRevision+1)

	if sa.prevSnapshot.LastRevision == lastEtcdRevision {
		zap.S().Infof("No new events since last snapshot. Skipping initial delta snapshot.")
		return false, nil
	}

	// need to take a delta snapshot here, because etcd revision is
	// newer than latest snapshot revision. Also means, a subsequent
	// full snapshot will be required later
	for {
		select {
		case wr, ok := <-sa.watchCh:
			if !ok {
				return false, fmt.Errorf("watch channel closed")
			}
			if err := sa.handleDeltaWatchEvents(wr); err != nil {
				return false, err
			}

			lastWatchRevision := wr.Events[len(wr.Events)-1].Kv.ModRevision
			if lastWatchRevision >= lastEtcdRevision {
				return false, nil
			}
		case <-stopCh:
			sa.cleanupInMemoryEvents()
			return true, nil
		}
	}
}

func (sa *SnapAction) handleDeltaWatchEvents(wr clientv3.WatchResponse) error {
	if err := wr.Err(); err != nil {
		return err
	}
	// aggregate events
	for _, ev := range wr.Events {
		timedEvent := newEvent(ev)
		jsonByte, err := json.Marshal(timedEvent)
		if err != nil {
			return fmt.Errorf("failed to marshal events to json: %v", err)
		}
		if len(sa.events) == 0 {
			sa.events = append(sa.events, byte('['))
		} else {
			sa.events = append(sa.events, byte(','))
		}
		sa.events = append(sa.events, jsonByte...)
		sa.lastEventRevision = ev.Kv.ModRevision
	}
	zap.S().Debugf("Added events till revision: %d", sa.lastEventRevision)
	if len(sa.events) >= int(sa.policy.DeltaSnapshotMemoryLimit) {
		zap.S().Infof("Delta events memory crossed the memory limit: %d Bytes", len(sa.events))
		_, err := sa.takeDeltaSnapshotAndResetTimer()
		return err
	}
	return nil
}

func newEvent(e *clientv3.Event) *event {
	return &event{
		EtcdEvent: e,
		Time:      time.Now(),
	}
}

func (sa *SnapAction) snapshotEventHandler(stopCh <-chan struct{}) error {
	_, updateCancel := context.WithCancel(context.TODO())
	defer updateCancel()
	zap.S().Info("Starting the Snapshot EventHandler.")
	for {
		select {
		case isFinal := <-sa.fullSnapshotReqCh:
			s, err := sa.TakeFullSnapshotAndResetTimer(isFinal)
			res := result{
				Snapshot: s,
				Err:      err,
			}
			sa.fullSnapshotAckCh <- res
			if err != nil {
				return err
			}

		case <-sa.deltaSnapshotReqCh:
			s, err := sa.takeDeltaSnapshotAndResetTimer()
			res := result{
				Snapshot: s,
				Err:      err,
			}
			sa.deltaSnapshotAckCh <- res
			if err != nil {
				return err
			}

		case <-sa.fullSnapshotTimer.C:
			if _, err := sa.TakeFullSnapshotAndResetTimer(false); err != nil {
				return err
			}

		case <-sa.deltaSnapshotTimer.C:
			if sa.policy.DeltaSnapshotPeriod >= time.Second {
				if _, err := sa.takeDeltaSnapshotAndResetTimer(); err != nil {
					return err
				}
			}

		case wr, ok := <-sa.watchCh:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if err := sa.handleDeltaWatchEvents(wr); err != nil {
				return err
			}

		case <-stopCh:
			zap.S().Info("Closing the Snapshot EventHandler.")
			sa.cleanupInMemoryEvents()
			return nil
		}
	}
}

func (sa *SnapAction) resetFullSnapshotTimer() error {
	now := time.Now()
	effective := sa.schedule.Next(now)
	if effective.IsZero() {
		zap.S().Info("There are no backups scheduled for the future. Stopping now.")
		return fmt.Errorf("error in full snapshot schedule")
	}
	duration := effective.Sub(now)
	if sa.fullSnapshotTimer == nil {
		sa.fullSnapshotTimer = time.NewTimer(duration)
	} else {
		zap.S().Infof("Stopping full snapshot...")
		sa.fullSnapshotTimer.Stop()
		zap.S().Infof("Resetting full snapshot to run after %s", duration)
		sa.fullSnapshotTimer.Reset(duration)
	}
	zap.S().Infof("Will take next full snapshot at time: %s", effective)

	return nil
}

func (sa *SnapAction) checkStoreSecretUpdate() bool {
	zap.S().Info("checking the hash of store secret...")
	newStoreSecretHash, err := store.GetStoreSecretHash(sa.storeConfig)
	if err != nil {
		return true
	}

	if snapStoreHash[sa.storeConfig.Provider] == newStoreSecretHash {
		return false
	}

	snapStoreHash[sa.storeConfig.Provider] = newStoreSecretHash
	return true
}
