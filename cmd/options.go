package cmd

import (
	"github.com/miaojuncn/etcd-ops/pkg/etcdconnection"
	flag "github.com/spf13/pflag"
)

type snapshotOptions struct {
	etcdConnectionConfig *etcdconnection.EtcdConnectionConfig
}

func newSnapshotOptions() *snapshotOptions {
	return &snapshotOptions{
		etcdConnectionConfig: etcdconnection.NewEtcdConnectionConfig(),
	}
}

func (s *snapshotOptions) addFlags(fs *flag.FlagSet) {
	s.etcdConnectionConfig.AddFlags(fs)
}

func (s *snapshotOptions) validate() error {
	return s.etcdConnectionConfig.Validate()
}
