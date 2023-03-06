package types

import (
	"fmt"
	"time"

	flag "github.com/spf13/pflag"
)

const (
	// defaultDefragTimeout defines default timeout duration for ETCD defrag call during compaction of snapshots.
	defaultDefragTimeout time.Duration = 8 * time.Minute
	// defaultSnapshotTimeout defines default timeout duration for taking compacted FullSnapshot.
	defaultSnapshotTimeout time.Duration = 30 * time.Minute
)

// CompactConfig holds all configuration options related to `compact` subcommand.
type CompactConfig struct {
	NeedDefrag      bool          `json:"needDefrag,omitempty"`
	SnapshotTimeout time.Duration `json:"snapshotTimeout,omitempty"`
	DefragTimeout   time.Duration `json:"defragTimeout,omitempty"`
}

// NewCompactConfig returns the CompactorConfig.
func NewCompactConfig() *CompactConfig {
	return &CompactConfig{
		NeedDefrag:      true,
		SnapshotTimeout: defaultSnapshotTimeout,
		DefragTimeout:   defaultDefragTimeout,
	}
}

// AddFlags adds the flags to flagSet.
func (c *CompactConfig) AddFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.NeedDefrag, "defrag", c.NeedDefrag, "defrag after compaction")
	fs.DurationVar(&c.SnapshotTimeout, "etcd-snapshot-timeout", c.SnapshotTimeout, "timeout duration for taking compacted full snapshots")
	fs.DurationVar(&c.DefragTimeout, "etcd-defrag-timeout", c.DefragTimeout, "timeout duration for etcd defrag call during compaction.")
}

// Validate validates the config.
func (c *CompactConfig) Validate() error {
	if c.SnapshotTimeout <= 0 {
		return fmt.Errorf("snapshot timeout should be greater than zero")

	}
	if c.DefragTimeout <= 0 {
		return fmt.Errorf("etcd defrag timeout should be greater than zero")
	}
	return nil
}
