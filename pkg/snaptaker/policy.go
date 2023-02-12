package snaptaker

import (
	"github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

func (c *PolicyConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVarP(&c.FullSnapshotSchedule, "schedule", "s", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.DurationVar(&c.DeltaSnapshotPeriod, "delta-snapshot-period", c.DeltaSnapshotPeriod, "period after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.DurationVar(&c.GarbageCollectionPeriod, "garbage-collection-period", c.GarbageCollectionPeriod, "period for garbage collecting old backups")
	fs.UintVarP(&c.MaxBackups, "max-backups", "m", c.MaxBackups, "max number of previous backups to keep")
}

func (c *PolicyConfig) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		zap.S().Error("validate snapshot policy cron expression error.")
		return err
	}

	if c.DeltaSnapshotPeriod < DeltaSnapshotIntervalThreshold {
		zap.S().Infof("Found delta snapshot interval %s less than 1 second. Disabling delta snapshotting", c.DeltaSnapshotPeriod)
	}

	if c.DeltaSnapshotMemoryLimit < 1 {
		zap.S().Infof("Found delta snapshot memory limit %d bytes less than 1 byte. Setting it to default: %d ", c.DeltaSnapshotMemoryLimit, DefaultDeltaSnapMemoryLimit)
		c.DeltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}
	return nil
}
