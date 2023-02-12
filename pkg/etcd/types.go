package etcd

import (
	"io"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultEtcdConnectionEndpoint string        = "127.0.0.1:2379"
	DefaultEtcdConnectionTimeout  time.Duration = 30 * time.Second
	DefaultDefragTimeout          time.Duration = 8 * time.Minute
	DefaultSnapshotTimeout        time.Duration = 15 * time.Minute
	DefragRetryPeriod             time.Duration = 1 * time.Minute
)

type EtcdConnectionConfig struct {
	Endpoints          []string      `json:"endpoints"`
	ServiceEndpoints   []string      `json:"serviceEndpoints,omitempty"`
	Username           string        `json:"username,omitempty"`
	Password           string        `json:"password,omitempty"`
	ConnectionTimeout  time.Duration `json:"connectionTimeout,omitempty"`
	SnapshotTimeout    time.Duration `json:"snapshotTimeout,omitempty"`
	DefragTimeout      time.Duration `json:"defragTimeout,omitempty"`
	InsecureTransport  bool          `json:"insecureTransport,omitempty"`
	InsecureSkipVerify bool          `json:"insecureSkipVerify,omitempty"`
	CertFile           string        `json:"certFile,omitempty"`
	KeyFile            string        `json:"keyFile,omitempty"`
	CaFile             string        `json:"caFile,omitempty"`
	MaxCallSendMsgSize int           `json:"maxCallSendMsgSize,omitempty"`
}

func NewEtcdConnectionConfig() *EtcdConnectionConfig {
	return &EtcdConnectionConfig{
		Endpoints:          []string{defaultEtcdConnectionEndpoint},
		ConnectionTimeout:  DefaultEtcdConnectionTimeout,
		SnapshotTimeout:    DefaultSnapshotTimeout,
		DefragTimeout:      DefaultDefragTimeout,
		InsecureTransport:  true,
		InsecureSkipVerify: false,
	}
}

// ClusterCloser adds io.Closer to the clientv3.Cluster interface to enable closing the underlying resources.
type ClusterCloser interface {
	clientv3.Cluster
	io.Closer
}

// KVCloser adds io.Closer to the clientv3.KV interface to enable closing the underlying resources.
type KVCloser interface {
	clientv3.KV
	io.Closer
}

// MaintenanceCloser adds io.Closer to the clientv3.Maintenance interface to enable closing the underlying resources.
type MaintenanceCloser interface {
	clientv3.Maintenance
	io.Closer
}

// Factory interface defines a way to construct and close the client objects for different ETCD API.
type Factory interface {
	NewCluster() (ClusterCloser, error)
	NewKV() (KVCloser, error)
	NewMaintenance() (MaintenanceCloser, error)
	NewWatcher() (clientv3.Watcher, error) // clientv3.Watcher already supports io.Closer
}
type Options struct {
	UseServiceEndpoints bool
}

// Option is an interface for changing configuration in client options.
type Option interface {
	ApplyTo(*Options)
}

// var _ Option = (*UseServiceEndpoints)(nil)

// UseServiceEndpoints instructs the client to use the service endpoints instead of endpoints.
type UseServiceEndpoints bool

// ApplyTo applies this configuration to the given options.
func (u UseServiceEndpoints) ApplyTo(opt *Options) {
	opt.UseServiceEndpoints = bool(u)
}
