package etcd

import (
	"fmt"

	flag "github.com/spf13/pflag"
)

func (c *EtcdConnectionConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringSliceVarP(&c.Endpoints, "endpoints", "e", c.Endpoints, "comma separated list of etcd endpoints")
	fs.StringSliceVar(&c.ServiceEndpoints, "service-endpoints", c.ServiceEndpoints, "comma separated list of etcd endpoints that are used for etcd-ops to connect to etcd through a (Kubernetes) service")
	fs.StringVar(&c.Username, "etcd-username", c.Username, "etcd server username, if one is required")
	fs.StringVar(&c.Password, "etcd-password", c.Password, "etcd server password, if one is required")
	fs.DurationVar(&c.ConnectionTimeout, "etcd-connection-timeout", c.ConnectionTimeout, "etcd client connection timeout")
	fs.DurationVar(&c.SnapshotTimeout, "etcd-snapshot-timeout", c.SnapshotTimeout, "timeout duration for taking etcd snapshots")
	fs.DurationVar(&c.DefragTimeout, "etcd-defrag-timeout", c.DefragTimeout, "timeout duration for etcd defrag call")
	fs.BoolVar(&c.InsecureTransport, "insecure-transport", c.InsecureTransport, "disable transport security for client connections")
	fs.BoolVar(&c.InsecureSkipVerify, "insecure-skip-tls-verify", c.InsecureTransport, "skip server certificate verification")
	fs.StringVar(&c.CertFile, "cert", c.CertFile, "identify secure client using this TLS certificate file")
	fs.StringVar(&c.KeyFile, "key", c.KeyFile, "identify secure client using this TLS key file")
	fs.StringVar(&c.CaFile, "ca-cert", c.CaFile, "verify certificates of TLS-enabled secure servers using this CA bundle")
}

// Validate validates the config.
func (c *EtcdConnectionConfig) Validate() error {
	if c.ConnectionTimeout <= 0 {
		return fmt.Errorf("connection timeout should be greater than zero")
	}
	if c.SnapshotTimeout <= 0 {
		return fmt.Errorf("snapshot timeout should be greater than zero")
	}
	if c.SnapshotTimeout < c.ConnectionTimeout {
		return fmt.Errorf("snapshot timeout should be greater than or equal to connection timeout")
	}
	if c.DefragTimeout <= 0 {
		return fmt.Errorf("etcd defrag timeout should be greater than zero")
	}
	return nil
}
