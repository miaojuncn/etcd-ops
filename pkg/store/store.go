package store

import (
	"fmt"
	"path"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/types"
)

const (
	// chunkUploadTimeout is timeout for uploading chunk.
	chunkUploadTimeout = 180 * time.Second
	// providerConnectionTimeout is timeout for connection/short queries to cloud provider.
	providerConnectionTimeout = 30 * time.Second
	// downloadTimeout is timeout for downloading chunk.
	downloadTimeout = 5 * time.Minute

	TmpBackupFilePrefix = "bak-tmp-"

	// maxRetryAttempts indicates the number of attempts to be retried in case of failure to upload chunk.
	maxRetryAttempts = 5
)

type chunk struct {
	offset  int64
	size    int64
	attempt uint
	id      int
}
type chunkUploadResult struct {
	err   error
	chunk *chunk
}

// GetStore returns the store object for give storageProvider with specified container
func GetStore(config *types.StoreConfig) (types.Store, error) {
	if config.Bucket == "" {
		config.Bucket = types.DefaultLocalStore

	}

	if config.Prefix == "" {
		config.Prefix = types.DefaultPrefix
	}

	if config.Bucket == "" && config.Provider != "" && config.Provider != types.StoreProviderLocal {
		return nil, fmt.Errorf("storage bucket not specified")
	}

	if config.MaxParallelChunkUploads <= 0 {
		config.MaxParallelChunkUploads = 5
	}

	switch config.Provider {
	case types.StoreProviderLocal, "":
		return NewLocalStore(path.Join(config.Bucket, config.Prefix))
	// case types.StoreProviderS3:
	// 	return NewS3Store(config)
	case types.StoreProviderOSS:
		return NewOSSStore(config)
	default:
		return nil, fmt.Errorf("unsupported storage provider : %s", config.Provider)
	}
}

// GetStoreSecretHash returns the hash of object store secrets hash
func GetStoreSecretHash(config *types.StoreConfig) (string, error) {
	switch config.Provider {
	case types.StoreProviderLocal:
		return "", nil
	// case types.StoreProviderS3:
	// 	return S3SStoreHash(config)
	case types.StoreProviderOSS:
		return OSSStoreHash(config)
	default:
		return "", nil
	}
}
