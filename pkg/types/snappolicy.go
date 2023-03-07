package types

import (
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
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
	// DeltaSnapshotIntervalThreshold is interval between delta snapshot
	DeltaSnapshotIntervalThreshold = time.Second

	SnapActionInactive SnapActionState = 0
	SnapActionActive   SnapActionState = 1

	// DefaultGarbageCollectionPeriod is the default interval for garbage collection
	DefaultGarbageCollectionPeriod = time.Minute
	// GarbageCollectionPolicyLimitBased LimitBased or KeepAlways
	GarbageCollectionPolicyLimitBased = "LimitBased"
	GarbageCollectionPolicyKeepAlways = "KeepAlways"
)

type SnapActionState int

type SnapPolicyConfig struct {
	FullSnapshotSchedule     string        `json:"schedule,omitempty"`
	DeltaSnapshotPeriod      time.Duration `json:"deltaSnapshotPeriod,omitempty"`
	DeltaSnapshotMemoryLimit uint          `json:"deltaSnapshotMemoryLimit,omitempty"`
	GarbageCollectionPolicy  string        `json:"garbageCollectionPolicy,omitempty"`
	GarbageCollectionPeriod  time.Duration `json:"garbageCollectionPeriod,omitempty"`
	MaxBackups               uint          `json:"maxBackups,omitempty"`
}

func NewSnapPolicyConfig() *SnapPolicyConfig {
	return &SnapPolicyConfig{
		FullSnapshotSchedule:     DefaultFullSnapshotSchedule,
		DeltaSnapshotPeriod:      DefaultDeltaSnapshotInterval,
		DeltaSnapshotMemoryLimit: DefaultDeltaSnapMemoryLimit,
		GarbageCollectionPolicy:  GarbageCollectionPolicyLimitBased,
		GarbageCollectionPeriod:  DefaultGarbageCollectionPeriod,
		MaxBackups:               DefaultMaxBackups,
	}
}
func (c *SnapPolicyConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVarP(&c.FullSnapshotSchedule, "schedule", "s", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.DurationVar(&c.DeltaSnapshotPeriod, "delta-snapshot-period", c.DeltaSnapshotPeriod, "period after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.StringVar(&c.GarbageCollectionPolicy, "garbage-collection-policy", c.GarbageCollectionPolicy, "Policy for garbage collecting old backups, LimitBased or KeepAlways")
	fs.DurationVar(&c.GarbageCollectionPeriod, "garbage-collection-period", c.GarbageCollectionPeriod, "period for garbage collecting old backups")
	fs.UintVarP(&c.MaxBackups, "max-backups", "m", c.MaxBackups, "max number of previous backups to keep")
}

func (c *SnapPolicyConfig) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		zlog.Logger.Error("Validate snapshot policy cron expression error.")
		return err
	}

	if c.DeltaSnapshotPeriod < DeltaSnapshotIntervalThreshold {
		zlog.Logger.Infof("Found delta snapshot interval %s less than 1 second. Disabling delta snapshotting", c.DeltaSnapshotPeriod)
	}

	if c.DeltaSnapshotMemoryLimit < 1 {
		zlog.Logger.Infof("Found delta snapshot memory limit %d bytes less than 1 byte. Setting it to default: %d ", c.DeltaSnapshotMemoryLimit, DefaultDeltaSnapMemoryLimit)
		c.DeltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}
	return nil
}
