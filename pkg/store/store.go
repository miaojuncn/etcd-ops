package store

import (
	"fmt"
	"path"

	flag "github.com/spf13/pflag"
)

func (c *StoreConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Provider, "storage-provider", c.Provider, "snapshot storage cloud provider")
	fs.StringVar(&c.Bucket, "bucket", c.Bucket, "store bucket or directory")
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

// GetStore returns the store object for give storageProvider with specified container
func GetStore(config *StoreConfig) (Store, error) {
	if config.Bucket == "" {
		config.Bucket = defaultLocalStore

	}

	if config.Prefix == "" {
		config.Prefix = defaultPrefix
	}

	if config.Bucket == "" && config.Provider != "" && config.Provider != StoreProviderLocal {
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
	case StoreProviderLocal, "":
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
