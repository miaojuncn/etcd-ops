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

	TmpBackupFilePrefix = "etcd-bak-tmp-"

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

	// if len(config.TempDir) == 0 {
	// 	config.TempDir = path.Join("/tmp")
	// }
	// if _, err := os.Stat(config.TempDir); err != nil {
	// 	if os.IsNotExist(err) {
	// 		logrus.Infof("Temporary directory %s does not exist. Creating it...", config.TempDir)
	// 		if err := os.MkdirAll(config.TempDir, 0700); err != nil {
	// 			return nil, fmt.Errorf("failed to create temporary directory %s: %v", config.TempDir, err)
	// 		}
	// 	} else {
	// 		return nil, fmt.Errorf("failed to get file info of temporary directory %s: %v", config.TempDir, err)
	// 	}
	// }

	if config.MaxParallelChunkUploads <= 0 {
		config.MaxParallelChunkUploads = 5
	}

	switch config.Provider {
	case types.StoreProviderLocal, "":
		return NewLocalSnapStore(path.Join(config.Bucket, config.Prefix))
	// case brtypes.SnapstoreProviderS3:
	// 	return NewS3SnapStore(config)
	// case brtypes.SnapstoreProviderABS:
	// 	return NewABSSnapStore(config)
	// case brtypes.SnapstoreProviderGCS:
	// 	return NewGCSSnapStore(config)
	// case brtypes.SnapstoreProviderSwift:
	// 	return NewSwiftSnapStore(config)
	// case brtypes.SnapstoreProviderOSS:
	// 	return NewOSSSnapStore(config)
	// case brtypes.SnapstoreProviderECS:
	// 	return NewECSSnapStore(config)
	// case brtypes.SnapstoreProviderOCS:
	// 	return NewOCSSnapStore(config)
	// case brtypes.SnapstoreProviderFakeFailed:
	// 	return NewFailedSnapStore(), nil
	default:
		return nil, fmt.Errorf("unsupported storage provider : %s", config.Provider)
	}
}

// GetStoreSecretHash returns the hash of object store secrets hash
func GetStoreSecretHash(config *types.StoreConfig) (string, error) {
	switch config.Provider {
	case types.StoreProviderLocal:
		return "", nil
	// case brtypes.SnapstoreProviderS3:
	// 	return S3SnapStoreHash(config)
	// case brtypes.SnapstoreProviderABS:
	// 	return ABSSnapStoreHash(config)
	// case brtypes.SnapstoreProviderGCS:
	// 	return GCSSnapStoreHash(config)
	// case brtypes.SnapstoreProviderSwift:
	// 	return SwiftSnapStoreHash(config)
	// case types.StoreProviderOSS:
	// 	return OSSSnapStoreHash(config)
	// case brtypes.SnapstoreProviderOCS:
	// 	return OCSSnapStoreHash(config)
	default:
		return "", nil
	}
}
