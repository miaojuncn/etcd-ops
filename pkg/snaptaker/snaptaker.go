package snaptaker

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
	"github.com/miaojuncn/etcd-ops/pkg/snapshot"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func (st *SnapTaker) Run(stopCh <-chan struct{}, startWithFullSnapshot bool) error {
	defer st.stop()
	if startWithFullSnapshot {
		st.fullSnapshotTimer = time.NewTimer(0)
	} else {
		// for the case when snaptaker is run for the first time on
		// a fresh etcd with startWithFullSnapshot set to false, we need
		// to take the first delta snapshot(s) initially and then set
		// the full snapshot schedule
		if st.watchCh == nil {
			ssrStopped, err := st.CollectEventsSincePrevSnapshot(stopCh)
			if ssrStopped {
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if err := st.resetFullSnapshotTimer(); err != nil {
			return fmt.Errorf("failed to reset full snapshot timer: %v", err)
		}
	}

	st.deltaSnapshotTimer = time.NewTimer(DefaultDeltaSnapshotInterval)
	if st.policy.DeltaSnapshotPeriod >= DeltaSnapshotIntervalThreshold {
		st.deltaSnapshotTimer.Stop()
		st.deltaSnapshotTimer.Reset(st.policy.DeltaSnapshotPeriod)
	}

	return st.snapshotEventHandler(stopCh)
}

// TriggerFullSnapshot sends the events to take full snapshot. This is to
// trigger full snapshot externally out of regular schedule.
func (st *SnapTaker) TriggerFullSnapshot(ctx context.Context, isFinal bool) (*snapshot.Snapshot, error) {
	st.SnapTakerMutex.Lock()
	defer st.SnapTakerMutex.Unlock()

	if st.SnapTakerState != SnapTakerActive {
		return nil, fmt.Errorf("snaptaker is not active")
	}
	st.logger.Info("Triggering out of schedule full snapshot...")
	st.fullSnapshotReqCh <- isFinal
	res := <-st.fullSnapshotAckCh
	return res.Snapshot, res.Err
}

// TriggerDeltaSnapshot sends the events to take delta snapshot. This is to
// trigger delta snapshot externally out of regular schedule.
func (st *SnapTaker) TriggerDeltaSnapshot() (*snapshot.Snapshot, error) {
	st.SnapTakerMutex.Lock()
	defer st.SnapTakerMutex.Unlock()

	if st.SnapTakerState != SnapTakerActive {
		return nil, fmt.Errorf("snapshotter is not active")
	}
	if st.policy.DeltaSnapshotPeriod < DeltaSnapshotIntervalThreshold {
		return nil, fmt.Errorf("found delta snapshot interval %s less than %v. Delta snapshotting is disabled. ", st.policy.DeltaSnapshotPeriod.Duration, time.Duration(brtypes.DeltaSnapshotIntervalThreshold))
	}
	st.logger.Info("Triggering out of schedule delta snapshot...")
	st.deltaSnapshotReqCh <- emptyStruct
	res := <-st.deltaSnapshotAckCh
	return res.Snapshot, res.Err
}

// stop stops the snapshot. Once stopped any subsequent calls will not have any effect.
func (st *SnapTaker) stop() {
	zap.L().Info("Closing the Snapshot")

	if st.fullSnapshotTimer != nil {
		st.fullSnapshotTimer.Stop()
		st.fullSnapshotTimer = nil
	}
	if st.deltaSnapshotTimer != nil {
		st.deltaSnapshotTimer.Stop()
		st.deltaSnapshotTimer = nil
	}
	st.SetSnapTakerInactive()
	st.closeEtcdClient()
}

// SetSnapTakerInactive set the snapshotter state to Inactive.
func (st *SnapTaker) SetSnapTakerInactive() {
	st.SnapTakerMutex.Lock()
	defer st.SnapTakerMutex.Unlock()
	st.SnapTakerState = SnapTakerInactive
}

// SetSnapTakerActive set the snapshotter state to active.
func (st *SnapTaker) SetSnapTakerActive() {
	st.SnapTakerMutex.Lock()
	defer st.SnapTakerMutex.Unlock()
	st.SnapTakerState = SnapTakerActive
}

func (st *SnapTaker) closeEtcdClient() {
	if st.cancelWatch != nil {
		st.cancelWatch()
		st.cancelWatch = nil
	}
	if st.watchCh != nil {
		st.watchCh = nil
	}

	if st.etcdWatchClient != nil {
		if err := (*st.etcdWatchClient).Close(); err != nil {
			st.logger.Warnf("Error while closing etcd watch client connection, %v", err)
		}
		st.etcdWatchClient = nil
	}
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot
// timer as per the schedule.
func (st *SnapTaker) TakeFullSnapshotAndResetTimer(isFinal bool) (*snapshot.Snapshot, error) {
	st.logger.Infof("Taking scheduled full snapshot for time: %s", time.Now().Local())
	s, err := st.takeFullSnapshot(isFinal)
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		st.logger.Warnf("Taking scheduled full snapshot failed: %v", err)
		return nil, err
	}

	return s, st.resetFullSnapshotTimer()
}

// takeFullSnapshot will store full snapshot of etcd to brtypes.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (st *SnapTaker) takeFullSnapshot(isFinal bool) (*snapshot.Snapshot, error) {
	defer st.cleanupInMemoryEvents()
	// close previous watch and client.
	st.closeEtcdClient()

	var err error

	// Update the snapstore object before taking every full snapshot
	// Refer: https://github.com/gardener/etcd-backup-restore/issues/422
	st.store, err = store.GetStore(st.storeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
	}

	clientFactory := etcdutil.NewFactory(*st.etcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), st.etcdConnectionConfig.ConnectionTimeout.Duration)
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	// Refer: https://github.com/coreos/etcd/issues/9037
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision

	if st.prevSnapshot.Kind == brtypes.SnapshotKindFull && st.prevSnapshot.LastRevision == lastRevision && st.prevSnapshot.IsFinal == isFinal {
		st.logger.Infof("There are no updates since last snapshot, skipping full snapshot.")
	} else {
		// Note: As FullSnapshot size can be very large, so to avoid context timeout use "SnapshotTimeout" in context.WithTimeout()
		ctx, cancel = context.WithTimeout(context.TODO(), st.etcdConnectionConfig.SnapshotTimeout.Duration)
		defer cancel()
		// compressionSuffix is useful in backward compatibility(restoring from uncompressed snapshots).
		// it is also helpful in inferring which compression Policy to be used to decompress the snapshot.
		compressionSuffix, err := compressor.GetCompressionSuffix(st.compressionConfig.Enabled, st.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
		}

		clientMaintenance, err := clientFactory.NewMaintenance()
		if err != nil {
			return nil, fmt.Errorf("failed to build etcd maintenance client")
		}
		defer clientMaintenance.Close()

		s, err := etcdutil.TakeAndSaveFullSnapshot(ctx, clientMaintenance, st.store, lastRevision, st.compressionConfig, compressionSuffix, isFinal, st.logger)
		if err != nil {
			return nil, err
		}

		st.prevSnapshot = s
		st.PrevFullSnapshot = s
		st.PrevDeltaSnapshots = nil

		metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: st.prevSnapshot.Kind}).Set(float64(st.prevSnapshot.LastRevision))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: st.prevSnapshot.Kind}).Set(float64(st.prevSnapshot.CreatedOn.Unix()))
		metrics.SnapstoreLatestDeltasTotal.With(prometheus.Labels{}).Set(0)
		metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)

		st.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}
	// setting `snapshotRequired` to 0 for both full and delta snapshot
	// for the following cases:
	// i.  Skipped full snapshot since no events were collected
	// ii. Successfully took a full snapshot
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)

	if st.config.DeltaSnapshotPeriod.Duration < time.Second {
		// return without creating a watch on events
		return st.prevSnapshot, nil
	}

	ssrEtcdWatchClient, err := clientFactory.NewWatcher()
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd watch client for snapshotter: %v", err),
		}
	}
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	st.cancelWatch = cancelWatch
	st.etcdWatchClient = &ssrEtcdWatchClient
	st.watchCh = ssrEtcdWatchClient.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(st.prevSnapshot.LastRevision+1))
	st.logger.Infof("Applied watch on etcd from revision: %d", st.prevSnapshot.LastRevision+1)

	return st.prevSnapshot, nil
}

func (st *SnapTaker) cleanupInMemoryEvents() {
	st.events = []byte{}
	st.lastEventRevision = -1
}

func (st *SnapTaker) takeDeltaSnapshotAndResetTimer() (*snapshot.Snapshot, error) {
	s, err := st.TakeDeltaSnapshot()
	if err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		st.logger.Warnf("Taking delta snapshot failed: %v", err)
		return nil, err
	}

	if st.deltaSnapshotTimer == nil {
		st.deltaSnapshotTimer = time.NewTimer(st.config.DeltaSnapshotPeriod.Duration)
	} else {
		st.logger.Infof("Stopping delta snapshot...")
		st.deltaSnapshotTimer.Stop()
		st.logger.Infof("Resetting delta snapshot to run after %s.", st.config.DeltaSnapshotPeriod.Duration.String())
		st.deltaSnapshotTimer.Reset(st.config.DeltaSnapshotPeriod.Duration)
	}
	return s, nil
}

// TakeDeltaSnapshot takes a delta snapshot that contains
// the etcd events collected up till now
func (st *SnapTaker) TakeDeltaSnapshot() (*snapshot.Snapshot, error) {
	defer st.cleanupInMemoryEvents()
	st.logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(st.events) == 0 {
		st.logger.Infof("No events received to save snapshot. Skipping delta snapshot.")
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)
		return nil, nil
	}
	st.events = append(st.events, byte(']'))

	isSecretUpdated := st.checkSnapstoreSecretUpdate()
	if isSecretUpdated {
		var err error

		// Update the snapstore object before taking every delta snapshot
		// Refer: https://github.com/gardener/etcd-backup-restore/issues/422
		st.store, err = snapstore.GetSnapstore(st.snapstoreConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
		}
		st.logger.Info("updated the snapstore object with new credentials")
	}

	// compressionSuffix is useful in backward compatibility(restoring from uncompressed snapshots).
	// it is also helpful in inferring which compression Policy to be used to decompress the snapshot.
	compressionSuffix, err := compressor.GetCompressionSuffix(st.compressionConfig.Enabled, st.compressionConfig.CompressionPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to get compressionSuffix: %v", err)
	}
	snap := snapstore.NewSnapshot(brtypes.SnapshotKindDelta, st.prevSnapshot.LastRevision+1, st.lastEventRevision, compressionSuffix, false)

	// compute hash
	hash := sha256.New()
	if _, err := hash.Write(st.events); err != nil {
		return nil, fmt.Errorf("failed to compute hash of events: %v", err)
	}
	st.events = hash.Sum(st.events)

	startTime := time.Now()
	rc := io.NopCloser(bytes.NewReader(st.events))

	// if compression is enabled
	//    then compress the snapshot.
	if st.compressionConfig.Enabled {
		st.logger.Info("start the Compression of delta snapshot")
		rc, err = compressor.CompressSnapshot(rc, st.compressionConfig.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to compress delta snapshot: %v", err)
		}
	}
	defer rc.Close()

	if err := st.store.Save(*snap, rc); err != nil {
		timeTaken := time.Now().Sub(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		st.logger.Errorf("Error saving delta snapshots. %v", err)
		return nil, err
	}
	timeTaken := time.Now().Sub(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	logrus.Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	st.prevSnapshot = snap
	st.PrevDeltaSnapshots = append(st.PrevDeltaSnapshots, snap)

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: st.prevSnapshot.Kind}).Set(float64(st.prevSnapshot.LastRevision))
	metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: st.prevSnapshot.Kind}).Set(float64(st.prevSnapshot.CreatedOn.Unix()))
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)
	metrics.SnapstoreLatestDeltasTotal.With(prometheus.Labels{}).Inc()
	metrics.SnapstoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Add(float64(snap.LastRevision - snap.StartRevision))

	st.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return snap, nil
}

// CollectEventsSincePrevSnapshot takes the first delta snapshot on etcd startup.
func (st *SnapTaker) CollectEventsSincePrevSnapshot(stopCh <-chan struct{}) (bool, error) {
	// close any previous watch and client.
	st.closeEtcdClient()

	clientFactory := etcdutil.NewFactory(*st.etcdConnectionConfig)
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd KV client: %v", err),
		}
	}
	defer clientKV.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), st.etcdConnectionConfig.ConnectionTimeout.Duration)
	resp, err := clientKV.Get(ctx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastEtcdRevision := resp.Header.Revision

	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(0)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(0)

	// if etcd revision newer than latest full snapshot revision,
	// set `required` metric for full snapshot to 1
	if st.PrevFullSnapshot == nil || st.PrevFullSnapshot.LastRevision != lastEtcdRevision {
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)
	}

	ssrEtcdWatchClient, err := clientFactory.NewWatcher()
	if err != nil {
		return false, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd watch client for snapshotter: %v", err),
		}
	}
	// TODO: Use parent context. Passing parent context here directly requires some additional management of error handling.
	watchCtx, cancelWatch := context.WithCancel(context.TODO())
	st.cancelWatch = cancelWatch
	st.etcdWatchClient = &ssrEtcdWatchClient
	st.watchCh = ssrEtcdWatchClient.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(st.prevSnapshot.LastRevision+1))
	st.logger.Infof("Applied watch on etcd from revision: %d", st.prevSnapshot.LastRevision+1)

	if st.prevSnapshot.LastRevision == lastEtcdRevision {
		st.logger.Infof("No new events since last snapshot. Skipping initial delta snapshot.")
		return false, nil
	}

	// need to take a delta snapshot here, because etcd revision is
	// newer than latest snapshot revision. Also means, a subsequent
	// full snapshot will be required later
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(1)
	metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)

	for {
		select {
		case wr, ok := <-st.watchCh:
			if !ok {
				return false, fmt.Errorf("watch channel closed")
			}
			if err := st.handleDeltaWatchEvents(wr); err != nil {
				return false, err
			}

			lastWatchRevision := wr.Events[len(wr.Events)-1].Kv.ModRevision
			if lastWatchRevision >= lastEtcdRevision {
				return false, nil
			}
		case <-stopCh:
			st.cleanupInMemoryEvents()
			return true, nil
		}
	}
}

func (st *SnapTaker) handleDeltaWatchEvents(wr clientv3.WatchResponse) error {
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
		if len(st.events) == 0 {
			st.events = append(st.events, byte('['))
		} else {
			st.events = append(st.events, byte(','))
		}
		st.events = append(st.events, jsonByte...)
		st.lastEventRevision = ev.Kv.ModRevision
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindFull}).Set(1)
		metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: brtypes.SnapshotKindDelta}).Set(1)
	}
	st.logger.Debugf("Added events till revision: %d", st.lastEventRevision)
	if len(st.events) >= int(st.config.DeltaSnapshotMemoryLimit) {
		st.logger.Infof("Delta events memory crossed the memory limit: %d Bytes", len(st.events))
		_, err := st.takeDeltaSnapshotAndResetTimer()
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

func (st *SnapTaker) snapshotEventHandler(stopCh <-chan struct{}) error {
	leaseUpdateCtx, leaseUpdateCancel := context.WithCancel(context.TODO())
	defer leaseUpdateCancel()
	st.logger.Info("Starting the Snapshot EventHandler.")
	for {
		select {
		case isFinal := <-st.fullSnapshotReqCh:
			s, err := st.TakeFullSnapshotAndResetTimer(isFinal)
			res := result{
				Snapshot: s,
				Err:      err,
			}
			st.fullSnapshotAckCh <- res
			if err != nil {
				return err
			}
			if st.healthConfig.SnapshotLeaseRenewalEnabled {
				ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
				if err = heartbeat.FullSnapshotCaseLeaseUpdate(ctx, st.logger, st.PrevFullSnapshot, st.K8sClientset, st.healthConfig.FullSnapshotLeaseName, st.healthConfig.DeltaSnapshotLeaseName); err != nil {
					st.logger.Warnf("Snapshot lease update failed : %v", err)
				}
				cancel()
			}

		case <-st.deltaSnapshotReqCh:
			s, err := st.takeDeltaSnapshotAndResetTimer()
			res := result{
				Snapshot: s,
				Err:      err,
			}
			st.deltaSnapshotAckCh <- res
			if err != nil {
				return err
			}
			if st.healthConfig.SnapshotLeaseRenewalEnabled {
				ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
				if err = heartbeat.DeltaSnapshotCaseLeaseUpdate(ctx, st.logger, st.K8sClientset, st.healthConfig.DeltaSnapshotLeaseName, st.store); err != nil {
					st.logger.Warnf("Snapshot lease update failed : %v", err)
				}
				cancel()
			}

		case <-st.fullSnapshotTimer.C:
			if _, err := st.TakeFullSnapshotAndResetTimer(false); err != nil {
				return err
			}
			if st.healthConfig.SnapshotLeaseRenewalEnabled {
				ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
				if err := heartbeat.FullSnapshotCaseLeaseUpdate(ctx, st.logger, st.PrevFullSnapshot, st.K8sClientset, st.healthConfig.FullSnapshotLeaseName, st.healthConfig.DeltaSnapshotLeaseName); err != nil {
					st.logger.Warnf("Snapshot lease update failed : %v", err)
				}
				cancel()
			}

		case <-st.deltaSnapshotTimer.C:
			if st.config.DeltaSnapshotPeriod.Duration >= time.Second {
				if _, err := st.takeDeltaSnapshotAndResetTimer(); err != nil {
					return err
				}
				if st.healthConfig.SnapshotLeaseRenewalEnabled {
					ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
					if err := heartbeat.DeltaSnapshotCaseLeaseUpdate(ctx, st.logger, st.K8sClientset, st.healthConfig.DeltaSnapshotLeaseName, st.store); err != nil {
						st.logger.Warnf("Snapshot lease update failed : %v", err)
					}
					cancel()
				}
			}

		case wr, ok := <-st.watchCh:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			snapshots := len(st.PrevDeltaSnapshots)
			if err := st.handleDeltaWatchEvents(wr); err != nil {
				return err
			}
			if st.healthConfig.SnapshotLeaseRenewalEnabled {
				// Call UpdateDeltaSnapshotLease only if new delta snapshot taken
				if snapshots < len(st.PrevDeltaSnapshots) {
					ctx, cancel := context.WithTimeout(leaseUpdateCtx, brtypes.LeaseUpdateTimeoutDuration)
					if err := heartbeat.DeltaSnapshotCaseLeaseUpdate(ctx, st.logger, st.K8sClientset, st.healthConfig.DeltaSnapshotLeaseName, st.store); err != nil {
						st.logger.Warnf("Snapshot lease update failed : %v", err)
					}
					cancel()
				}
			}

		case <-stopCh:
			st.logger.Info("Closing the Snapshot EventHandler.")
			st.cleanupInMemoryEvents()
			return nil
		}
	}
}

func (st *SnapTaker) resetFullSnapshotTimer() error {
	now := time.Now()
	effective := st.schedule.Next(now)
	if effective.IsZero() {
		st.logger.Info("There are no backups scheduled for the future. Stopping now.")
		return fmt.Errorf("error in full snapshot schedule")
	}
	duration := effective.Sub(now)
	if st.fullSnapshotTimer == nil {
		st.fullSnapshotTimer = time.NewTimer(duration)
	} else {
		st.logger.Infof("Stopping full snapshot...")
		st.fullSnapshotTimer.Stop()
		st.logger.Infof("Resetting full snapshot to run after %s", duration)
		st.fullSnapshotTimer.Reset(duration)
	}
	st.logger.Infof("Will take next full snapshot at time: %s", effective)

	return nil
}

func (st *SnapTaker) checkSnapstoreSecretUpdate() bool {
	st.logger.Debug("checking the hash of snapstore secret...")
	newSnapstoreSecretHash, err := snapstore.GetSnapstoreSecretHash(st.snapstoreConfig)
	if err != nil {
		return true
	}

	if snapStoreHash[st.snapstoreConfig.Provider] == newSnapstoreSecretHash {
		return false
	}

	// update the map with latest newsnapStoreHash
	snapStoreHash[st.snapstoreConfig.Provider] = newSnapstoreSecretHash
	return true
}
