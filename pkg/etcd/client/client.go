package client

import (
	"io"

	clientv3 "go.etcd.io/etcd/client/v3"
)

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
