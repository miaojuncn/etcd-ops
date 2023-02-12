package snaptaker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	SnapshotKindFull  = "Full"
	SnapshotKindDelta = "Incr"
	SnapshotKindChunk = "Chunk"

	DefaultMaxBackups = 24 * 10
	// DefaultDeltaSnapMemoryLimit is default memory limit for delta snapshots.
	DefaultDeltaSnapMemoryLimit = 10 * 1024 * 1024 // 10Mib
	// DefaultDeltaSnapshotInterval is the default interval for delta snapshots.
	DefaultDeltaSnapshotInterval = 20 * time.Second
	// DefaultFullSnapshotSchedule is the default schedule
	DefaultFullSnapshotSchedule = "*/30 * * * *"
	// DefaultGarbageCollectionPeriod is the default interval for garbage collection
	DefaultGarbageCollectionPeriod = time.Minute
	// DeltaSnapshotIntervalThreshold is interval between delta snapshot
	DeltaSnapshotIntervalThreshold = time.Second * 30

	SnapTakerInactive SnapTakerState = 0
	SnapTakerActive   SnapTakerState = 1
)

var (
	emptyStruct   struct{}
	snapStoreHash = make(map[string]interface{})
)

type PolicyConfig struct {
	FullSnapshotSchedule     string        `json:"schedule,omitempty"`
	DeltaSnapshotPeriod      time.Duration `json:"deltaSnapshotPeriod,omitempty"`
	DeltaSnapshotMemoryLimit uint          `json:"deltaSnapshotMemoryLimit,omitempty"`
	GarbageCollectionPeriod  time.Duration `json:"garbageCollectionPeriod,omitempty"`
	MaxBackups               uint          `json:"maxBackups,omitempty"`
}

func NewPolicyConfig() *PolicyConfig {
	return &PolicyConfig{
		FullSnapshotSchedule:     DefaultFullSnapshotSchedule,
		DeltaSnapshotPeriod:      DefaultDeltaSnapshotInterval,
		DeltaSnapshotMemoryLimit: DefaultDeltaSnapMemoryLimit,
		GarbageCollectionPeriod:  DefaultGarbageCollectionPeriod,
		MaxBackups:               DefaultMaxBackups,
	}
}

type SnapTakerState int

type result struct {
	Snapshot *snapshot.Snapshot `json:"snapshot"`
	Err      error              `json:"error"`
}

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

type SnapTaker struct {
	etcdConnectionConfig *etcd.EtcdConnectionConfig
	store                store.Store
	policy               *PolicyConfig
	compressionConfig    *compressor.CompressionConfig
	schedule             cron.Schedule
	prevSnapshot         *snapshot.Snapshot
	PrevFullSnapshot     *snapshot.Snapshot
	PrevDeltaSnapshots   snapshot.SnapList
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
	SnapTakerMutex       *sync.Mutex
	SnapTakerState       SnapTakerState
	lastEventRevision    int64
	K8sClientSet         client.Client
	storeConfig          *store.StoreConfig
}

func NewSnapTaker(etcdConnectionConfig *etcd.EtcdConnectionConfig, policy *PolicyConfig,
	compressionConfig *compressor.CompressionConfig, storeConfig *store.StoreConfig, store store.Store) (*SnapTaker, error) {

	sdl, err := cron.ParseStandard(policy.FullSnapshotSchedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", policy.FullSnapshotSchedule, err)
	}

	var prevSnapshot *snapshot.Snapshot
	fullSnap, deltaSnapList, err := tools.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		return nil, err
	} else if fullSnap != nil && len(deltaSnapList) == 0 {
		prevSnapshot = fullSnap
	} else if fullSnap != nil && len(deltaSnapList) != 0 {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
	} else {
		prevSnapshot = snapshot.NewSnapshot(SnapshotKindFull, 0, 0, "")
	}
	return &SnapTaker{
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
		SnapTakerMutex:    &sync.Mutex{},
		SnapTakerState:    SnapTakerInactive,
		lastEventRevision: 0,
		storeConfig:       storeConfig,
	}, nil
}
