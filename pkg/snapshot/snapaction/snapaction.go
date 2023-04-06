package snapaction

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/errors"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/metrics"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	snapStoreHash = make(map[string]interface{})
)

type SnapAction struct {
	etcdConnectionConfig *types.EtcdConnectionConfig
	store                types.Store
	policy               *types.SnapPolicyConfig
	compressionConfig    *types.CompressionConfig
	schedule             cron.Schedule
	prevSnapshot         *types.Snapshot
	PrevFullSnapshot     *types.Snapshot
	PrevDeltaSnapshots   types.SnapList
	fullSnapshotTimer    *time.Timer
	deltaSnapshotTimer   *time.Timer
	events               []byte
	watchCh              clientv3.WatchChan
	etcdWatchClient      *clientv3.Watcher
	cancelWatch          context.CancelFunc
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
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull}).Set(float64(prevSnapshot.CreatedOn.Unix()))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else if fullSnap != nil && len(deltaSnapList) != 0 {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull}).Set(float64(fullSnap.CreatedOn.Unix()))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else {
		prevSnapshot = types.NewSnapshot(types.SnapshotKindFull, 0, 0, "")
	}
	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.LastRevision))

	return &SnapAction{
		etcdConnectionConfig: etcdConnectionConfig,
		store:                store,
		policy:               policy,
		compressionConfig:    compressionConfig,
		schedule:             sdl,
		prevSnapshot:         prevSnapshot,
		PrevFullSnapshot:     fullSnap,
		PrevDeltaSnapshots:   deltaSnapList,
		cancelWatch:          func() {},
		lastEventRevision:    0,
		storeConfig:          storeConfig,
	}, nil
}

func (sa *SnapAction) Run(stopCh <-chan struct{}) error {
	defer sa.stop()

	sa.fullSnapshotTimer = time.NewTimer(0)
	sa.deltaSnapshotTimer = time.NewTimer(types.DefaultDeltaSnapshotInterval)

	if sa.policy.DeltaSnapshotPeriod >= types.DeltaSnapshotIntervalThreshold {
		sa.deltaSnapshotTimer.Reset(sa.policy.DeltaSnapshotPeriod)
	}

	return sa.snapshotEventHandler(stopCh)
}

// stop stops the snapshot. Once stopped any subsequent calls will not have any effect.
func (sa *SnapAction) stop() {
	zlog.Logger.Info("Closing the Snapshot")

	if sa.fullSnapshotTimer != nil {
		sa.fullSnapshotTimer.Stop()
		sa.fullSnapshotTimer = nil
	}
	if sa.deltaSnapshotTimer != nil {
		sa.deltaSnapshotTimer.Stop()
		sa.deltaSnapshotTimer = nil
	}
	sa.closeEtcdClient()
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
			zlog.Logger.Warnf("Error while closing etcd watch client connection, %v", err)
		}
		sa.etcdWatchClient = nil
	}
}

// takeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot timer as per the schedule.
func (sa *SnapAction) takeFullSnapshotAndResetTimer() (*types.Snapshot, error) {
	zlog.Logger.Infof("Taking scheduled full snapshot for time: %s", time.Now().Local())
	s, err := sa.takeFullSnapshot()
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		zlog.Logger.Warnf("Taking scheduled full snapshot failed: %v", err)
		return nil, err
	}

	return s, sa.resetFullSnapshotTimer()
}

// takeFullSnapshot will store full snapshot of etcd.
// It basically will connect to etcd. Then ask for snapshot. And finally store it to underlying snap store on the fly.
func (sa *SnapAction) takeFullSnapshot() (*types.Snapshot, error) {
	defer sa.cleanupInMemoryEvents()
	// close previous watch and client.
	sa.closeEtcdClient()

	var err error

	// Update the store object before taking every full snapshot
	sa.store, err = store.GetStore(sa.storeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snap store from configured storage provider: %v", err)
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

	if sa.prevSnapshot.Kind == types.SnapshotKindFull && sa.prevSnapshot.LastRevision == lastRevision {
		zlog.Logger.Info("There are no updates since last snapshot, skipping full snapshot.")
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

		s, err := etcd.TakeAndSaveFullSnapshot(ctx, clientMaintenance, sa.store, lastRevision, sa.compressionConfig, compressionSuffix)
		if err != nil {
			return nil, err
		}

		sa.prevSnapshot = s
		sa.PrevFullSnapshot = s
		sa.PrevDeltaSnapshots = nil

		metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: sa.prevSnapshot.Kind}).Set(float64(sa.prevSnapshot.LastRevision))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: sa.prevSnapshot.Kind}).Set(float64(sa.prevSnapshot.CreatedOn.Unix()))
		metrics.StoreLatestDeltasTotal.With(prometheus.Labels{}).Set(0)
		metrics.StoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)

		zlog.Logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}

	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta}).Set(0)
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
	zlog.Logger.Infof("Applied watch on etcd from revision: %d", sa.prevSnapshot.LastRevision+1)

	return sa.prevSnapshot, nil
}

func (sa *SnapAction) cleanupInMemoryEvents() {
	sa.events = []byte{}
	sa.lastEventRevision = -1
}

func (sa *SnapAction) takeDeltaSnapshotAndResetTimer() (*types.Snapshot, error) {
	s, err := sa.TakeDeltaSnapshot()
	if err != nil {
		zlog.Logger.Infof("Taking delta snapshot failed: %v", err)
		return nil, err
	}

	if sa.deltaSnapshotTimer == nil {
		sa.deltaSnapshotTimer = time.NewTimer(sa.policy.DeltaSnapshotPeriod)
	} else {
		zlog.Logger.Info("Stopping delta snapshot...")
		sa.deltaSnapshotTimer.Stop()
		zlog.Logger.Infof("Resetting delta snapshot to run after %s.", sa.policy.DeltaSnapshotPeriod.String())
		sa.deltaSnapshotTimer.Reset(sa.policy.DeltaSnapshotPeriod)
	}
	return s, nil
}

// TakeDeltaSnapshot takes a delta snapshot that contains the etcd events collected up till now
func (sa *SnapAction) TakeDeltaSnapshot() (*types.Snapshot, error) {
	defer sa.cleanupInMemoryEvents()
	zlog.Logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(sa.events) == 0 {
		zlog.Logger.Info("No events received to save snapshot. Skipping delta snapshot.")
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta}).Set(0)
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
		zlog.Logger.Info("Updated the snap store object with new credentials")
	}

	compressionSuffix, err := compressor.GetCompressionSuffix(sa.compressionConfig.Enabled, sa.compressionConfig.CompressionPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
	}
	snap := types.NewSnapshot(types.SnapshotKindDelta, sa.prevSnapshot.LastRevision+1, sa.lastEventRevision, compressionSuffix)

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
		zlog.Logger.Info("Start the Compression of delta snapshot")
		rc, err = compressor.CompressSnapshot(rc, sa.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to compress delta snapshot: %v", err)
		}
	}
	defer rc.Close()

	if err := sa.store.Save(*snap, rc); err != nil {
		timeTaken := time.Since(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		zlog.Logger.Errorf("Error saving delta snapshots. %v", err)
		return nil, err
	}
	timeTaken := time.Since(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	zlog.Logger.Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	sa.prevSnapshot = snap
	sa.PrevDeltaSnapshots = append(sa.PrevDeltaSnapshots, snap)

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: sa.prevSnapshot.Kind}).Set(float64(sa.prevSnapshot.LastRevision))
	metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: sa.prevSnapshot.Kind}).Set(float64(sa.prevSnapshot.CreatedOn.Unix()))
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta}).Set(0)
	metrics.StoreLatestDeltasTotal.With(prometheus.Labels{}).Inc()
	metrics.StoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Add(float64(snap.LastRevision - snap.StartRevision))

	zlog.Logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return snap, nil
}

func (sa *SnapAction) handleDeltaWatchEvents(wr clientv3.WatchResponse) error {
	if err := wr.Err(); err != nil {
		return err
	}
	// aggregate events
	for _, ev := range wr.Events {
		timedEvent := types.NewEvent(ev)
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
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull}).Set(1)
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindDelta}).Set(1)

	}
	zlog.Logger.Debugf("Added events till revision: %d", sa.lastEventRevision)
	if len(sa.events) >= int(sa.policy.DeltaSnapshotMemoryLimit) {
		zlog.Logger.Infof("Delta events memory crossed the memory limit: %d Bytes", len(sa.events))
		_, err := sa.takeDeltaSnapshotAndResetTimer()
		return err
	}
	return nil
}

func (sa *SnapAction) snapshotEventHandler(stopCh <-chan struct{}) error {
	_, updateCancel := context.WithCancel(context.TODO())
	defer updateCancel()
	zlog.Logger.Info("Starting the Snapshot EventHandler.")
	for {
		select {
		case <-sa.fullSnapshotTimer.C:
			if _, err := sa.takeFullSnapshotAndResetTimer(); err != nil {
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
			zlog.Logger.Info("Closing the Snapshot EventHandler.")
			sa.cleanupInMemoryEvents()
			return nil
		}
	}
}

func (sa *SnapAction) resetFullSnapshotTimer() error {
	now := time.Now()
	effective := sa.schedule.Next(now)
	if effective.IsZero() {
		zlog.Logger.Info("There are no snapshot scheduled for the future. Stopping now.")
		return fmt.Errorf("error in full snapshot schedule")
	}
	duration := effective.Sub(now)
	if sa.fullSnapshotTimer == nil {
		sa.fullSnapshotTimer = time.NewTimer(duration)
	} else {
		zlog.Logger.Infof("Stopping full snapshot...")
		sa.fullSnapshotTimer.Stop()
		zlog.Logger.Infof("Resetting full snapshot to run after %s", duration)
		sa.fullSnapshotTimer.Reset(duration)
	}
	zlog.Logger.Infof("Will take next full snapshot at time: %s", effective)

	return nil
}

func (sa *SnapAction) checkStoreSecretUpdate() bool {
	zlog.Logger.Info("Checking the hash of store secret...")
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
