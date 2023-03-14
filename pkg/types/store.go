package types

import (
	"fmt"
	"io"

	flag "github.com/spf13/pflag"
)

const (
	StoreProviderLocal       = "Local"
	StoreProviderS3          = "S3"
	StoreProviderOSS         = "OSS"
	MinChunkSize       int64 = 5 * (1 << 20) // 5 MiB
	DefaultLocalStore        = "default.bak"
	DefaultPrefix            = "etcd.bak"
)

type Store interface {
	// Fetch should open reader for the snapshot file from store.
	Fetch(Snapshot) (io.ReadCloser, error)
	// List will return sorted list with all snapshot files on store.
	List() (SnapList, error)
	// Save will write the snapshot to store.
	Save(Snapshot, io.ReadCloser) error
	// Delete should delete the snapshot file from store.
	Delete(Snapshot) error
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

func (c *StoreConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Provider, "storage-provider", c.Provider, "snapshot storage cloud provider")
	fs.StringVar(&c.Bucket, "bucket", c.Bucket, "store bucket or directory on host")
	fs.StringVar(&c.Prefix, "prefix", c.Prefix, "directory under bucket")
	fs.UintVar(&c.MaxParallelChunkUploads, "max-parallel-chunk-uploads", c.MaxParallelChunkUploads, "max number of parallel chunk uploads allowed")
	fs.Int64Var(&c.MinChunkSize, "min-chunk-size", c.MinChunkSize, "min size for multipart chunk upload")
}

// Validate validates the config.
func (c *StoreConfig) Validate() error {
	if c.MaxParallelChunkUploads <= 0 {
		return fmt.Errorf("max parallel chunk uploads should be greater than zero")
	}
	if c.MinChunkSize < MinChunkSize {
		return fmt.Errorf("min chunk size for multi-part chunk upload should be greater than or equal to 5 MiB")
	}
	return nil
}
