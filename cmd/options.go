package cmd

import (
	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	flag "github.com/spf13/pflag"
)

type snapshotOptions struct {
	etcdConnectionConfig *etcd.EtcdConnectionConfig
	storeConfig          *store.StoreConfig
	compressionConfig    *compressor.CompressionConfig
}

func newSnapshotOptions() *snapshotOptions {
	return &snapshotOptions{
		etcdConnectionConfig: etcd.NewEtcdConnectionConfig(),
		storeConfig:          store.NewStoreConfig(),
		compressionConfig:    compressor.NewCompressorConfig(),
	}
}

func (s *snapshotOptions) addFlags(fs *flag.FlagSet) {
	s.etcdConnectionConfig.AddFlags(fs)
	s.storeConfig.AddFlags(fs)
	s.compressionConfig.AddFlags(fs)
}

func (s *snapshotOptions) validate() error {
	if err := s.storeConfig.Validate(); err != nil {
		return err
	}
	if err := s.compressionConfig.Validate(); err != nil {
		return err
	}
	return s.etcdConnectionConfig.Validate()
}
