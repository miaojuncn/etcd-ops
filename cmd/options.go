package cmd

import (
	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/snaptaker"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	flag "github.com/spf13/pflag"
)

type snapshotOptions struct {
	etcdConnectionConfig    *etcd.EtcdConnectionConfig
	storeConfig             *store.StoreConfig
	compressionConfig       *compressor.CompressionConfig
	policyConfig            *snaptaker.PolicyConfig
	defragmentationSchedule string
}

func newSnapshotOptions() *snapshotOptions {
	return &snapshotOptions{
		etcdConnectionConfig:    etcd.NewEtcdConnectionConfig(),
		storeConfig:             store.NewStoreConfig(),
		compressionConfig:       compressor.NewCompressorConfig(),
		policyConfig:            snaptaker.NewPolicyConfig(),
		defragmentationSchedule: "0 0 */3 * *",
	}
}

func (s *snapshotOptions) addFlags(fs *flag.FlagSet) {
	s.etcdConnectionConfig.AddFlags(fs)
	s.storeConfig.AddFlags(fs)
	s.policyConfig.AddFlags(fs)
	s.compressionConfig.AddFlags(fs)
	fs.StringVar(&s.defragmentationSchedule, "defragmentation-schedule", s.defragmentationSchedule, "schedule to defragment etcd data directory")
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
