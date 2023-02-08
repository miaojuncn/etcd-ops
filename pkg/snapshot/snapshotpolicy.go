package snapshot

import (
	"time"

	"github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
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

func (c *PolicyConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVarP(&c.FullSnapshotSchedule, "schedule", "s", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.DurationVar(&c.DeltaSnapshotPeriod, "delta-snapshot-period", c.DeltaSnapshotPeriod, "period after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.DurationVar(&c.GarbageCollectionPeriod, "garbage-collection-period", c.GarbageCollectionPeriod, "period for garbage collecting old backups")
	fs.UintVarP(&c.MaxBackups, "max-backups", "m", c.MaxBackups, "max number of previous backups to keep")
}

func (c *PolicyConfig) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		zap.L().Error("validate snapshot policy cron expression error.")
		return err
	}

	if c.DeltaSnapshotPeriod < DeltaSnapshotIntervalThreshold {
		zap.L().Info("found delta snapshot interval %s less than 1 second, disabling delta snapshotting.")
	}

	if c.DeltaSnapshotMemoryLimit < 1 {
		zap.L().Info("found delta snapshot memory limit bytes less than 1 byte, set it to default.")
		c.DeltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}
	return nil
}
