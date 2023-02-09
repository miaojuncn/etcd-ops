package store

import (
	"io"

	"github.com/miaojuncn/etcd-ops/pkg/snapshot"
)

const (
	StoreProviderLocal       = "Local"
	StoreProviderS3          = "S3"
	StoreProviderOSS         = "OSS"
	MinChunkSize       int64 = 5 * (1 << 20) // 5 MiB
	defaultLocalStore        = "default.bkp"
	defaultPrefix            = "etcd"
)

type Store interface {
	// Fetch should open reader for the snapshot file from store.
	Fetch(snapshot.Snapshot) (io.ReadCloser, error)
	// List will return sorted list with all snapshot files on store.
	List() (snapshot.SnapList, error)
	// Save will write the snapshot to store.
	Save(snapshot.Snapshot, io.ReadCloser) error
	// Delete should delete the snapshot file from store.
	Delete(snapshot.Snapshot) error
}

// StoreConfig defines the configuration to create snapshot store.
type StoreConfig struct {
	// Provider indicated the cloud provider.
	Provider string `json:"provider,omitempty"`
	// Bucket indicated which bucket to store snapshot.
	Bucket string `json:"bucket"`
	// Prefix holds directory under bucket.
	Prefix string `json:"prefix"`
	// MaxParallelChunkUploads holds the maximum number of parallel chunk uploads allowed.
	MaxParallelChunkUploads uint `json:"maxParallelChunkUploads,omitempty"`
	// MinChunkSize holds the minimum size for a multi-part chunk upload.
	MinChunkSize int64 `json:"minChunkSize,omitempty"`
}

func NewStoreConfig() *StoreConfig {
	return &StoreConfig{
		MaxParallelChunkUploads: 5,
		MinChunkSize:            MinChunkSize,
	}
}
