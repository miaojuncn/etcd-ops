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
