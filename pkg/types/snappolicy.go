package types

import (
	"fmt"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/log"
	"github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	SnapshotKindFull  = "Full"
	SnapshotKindDelta = "Incr"

	DefaultMaxBackups = 24 * 10
	// DefaultDeltaSnapMemoryLimit is default memory limit for delta snapshots.
	DefaultDeltaSnapMemoryLimit = 10 * 1024 * 1024 // 10Mib
	// DefaultDeltaSnapshotInterval is the default interval for delta snapshots.
	DefaultDeltaSnapshotInterval = 30 * time.Second
	// DefaultFullSnapshotSchedule is the default schedule
	DefaultFullSnapshotSchedule = "*/30 * * * *"
	// DeltaSnapshotIntervalThreshold is interval between delta snapshot
	DeltaSnapshotIntervalThreshold = time.Second

	// DefaultGarbageCollectionPeriod is the default interval for garbage collection
	DefaultGarbageCollectionPeriod = time.Minute
	// GarbageCollectionPolicyLimitBased LimitBased or KeepAlways
	GarbageCollectionPolicyLimitBased = "LimitBased"
	GarbageCollectionPolicyKeepAlways = "KeepAlways"
)

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
	fs.StringVar(&c.FullSnapshotSchedule, "schedule", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.DurationVar(&c.DeltaSnapshotPeriod, "delta-snapshot-period", c.DeltaSnapshotPeriod, "period after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.StringVar(&c.GarbageCollectionPolicy, "garbage-collection-policy", c.GarbageCollectionPolicy, "Policy for garbage collecting old backups, LimitBased or KeepAlways")
	fs.DurationVar(&c.GarbageCollectionPeriod, "garbage-collection-period", c.GarbageCollectionPeriod, "period for garbage collecting old backups")
	fs.UintVar(&c.MaxBackups, "max-backups", c.MaxBackups, "max number of previous backups to keep")
}

func (c *SnapPolicyConfig) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		return fmt.Errorf("validate snapshot policy cron expression error")
	}

	if c.GarbageCollectionPolicy != GarbageCollectionPolicyLimitBased && c.GarbageCollectionPolicy != GarbageCollectionPolicyKeepAlways {
		return fmt.Errorf("invalid garbage collection policy: %s", c.GarbageCollectionPolicy)
	}
	if c.GarbageCollectionPolicy == GarbageCollectionPolicyLimitBased && c.MaxBackups <= 0 {
		return fmt.Errorf("max backups should be greather than zero for garbage collection policy set to limit based")
	}

	logger := log.NewLogger()
	if c.DeltaSnapshotPeriod < DeltaSnapshotIntervalThreshold {
		logger.Info("Found delta snapshot interval less than 1 second. Disabling delta snapshotting.")
	}

	if c.DeltaSnapshotMemoryLimit < 1 {
		logger.Info("Found delta snapshot memory limit bytes less than 1 byte. Setting it to default.",
			zap.Uint("set", c.DeltaSnapshotMemoryLimit), zap.Uint("default", DefaultDeltaSnapMemoryLimit))
		c.DeltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}
	return nil
}
