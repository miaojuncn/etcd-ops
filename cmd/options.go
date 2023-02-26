package cmd

import (
	"github.com/miaojuncn/etcd-ops/pkg/types"
	flag "github.com/spf13/pflag"
)

type snapshotOptions struct {
	etcdConnectionConfig *types.EtcdConnectionConfig
	storeConfig          *types.StoreConfig
	compressionConfig    *types.CompressionConfig
	policyConfig         *types.SnapPolicyConfig
	defragSchedule       string
}

func newSnapshotOptions() *snapshotOptions {
	return &snapshotOptions{
		etcdConnectionConfig: types.NewEtcdConnectionConfig(),
		storeConfig:          types.NewStoreConfig(),
		compressionConfig:    types.NewCompressorConfig(),
		policyConfig:         types.NewSnapPolicyConfig(),
		defragSchedule:       "0 0 */3 * *",
	}
}

func (s *snapshotOptions) addFlags(fs *flag.FlagSet) {
	s.etcdConnectionConfig.AddFlags(fs)
	s.storeConfig.AddFlags(fs)
	s.policyConfig.AddFlags(fs)
	s.compressionConfig.AddFlags(fs)
	fs.StringVar(&s.defragSchedule, "defrag-schedule", s.defragSchedule, "schedule to defrag etcd data directory")
}

func (s *snapshotOptions) validate() error {
	if err := s.storeConfig.Validate(); err != nil {
		return err
	}
	if err := s.policyConfig.Validate(); err != nil {
		return err
	}
	if err := s.compressionConfig.Validate(); err != nil {
		return err
	}
	return s.etcdConnectionConfig.Validate()
}

type restoreOptions struct {
	restoreConfig *types.RestoreConfig
	storeConfig   *types.StoreConfig
}

// newRestorerOptions returns the validation config.
func newRestoreOptions() *restoreOptions {
	return &restoreOptions{
		restoreConfig: types.NewRestoreConfig(),
		storeConfig:   types.NewStoreConfig(),
	}
}

// AddFlags adds the flags to flagSet.
func (c *restoreOptions) addFlags(fs *flag.FlagSet) {
	c.restoreConfig.AddFlags(fs)
	c.storeConfig.AddFlags(fs)
}

// Validate validates the config.
func (c *restoreOptions) validate() error {
	if err := c.storeConfig.Validate(); err != nil {
		return err
	}

	return c.restoreConfig.Validate()
}
