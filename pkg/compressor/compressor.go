package compressor

import (
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/miaojuncn/etcd-ops/pkg/log"
	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/types"
)

func GetCompressionSuffix(compressionEnabled bool, compressionPolicy string) (string, error) {

	if !compressionEnabled {
		return types.UnCompressSnapshotExtension, nil
	}

	switch compressionPolicy {
	case types.ZlibCompressionPolicy:
		return types.ZlibCompressionExtension, nil

	case types.GzipCompressionPolicy:
		return types.GzipCompressionExtension, nil

	default:
		return "", fmt.Errorf("unsupported Compression Policy")
	}
}

func IsSnapshotCompressed(compressionSuffix string) (bool, string, error) {

	switch compressionSuffix {
	case types.ZlibCompressionExtension:
		return true, types.ZlibCompressionPolicy, nil

	case types.GzipCompressionExtension:
		return true, types.GzipCompressionPolicy, nil

	case types.UnCompressSnapshotExtension:
		return false, "", nil

	// for unsupported CompressionPolicy return the error
	default:
		return false, "", fmt.Errorf("unsupported Compression Policy")
	}
}

// CompressSnapshot takes uncompressed data as input and compress the data according to Compression Policy
// and write the compressed data into one end of pipe.
func CompressSnapshot(data io.ReadCloser, compressionPolicy string) (io.ReadCloser, error) {
	logger := log.NewLogger()
	pReader, pWriter := io.Pipe()

	var gWriter io.WriteCloser
	logger.Info("start compressing the snapshot.", zap.String("compressionPolicy", compressionPolicy))

	switch compressionPolicy {
	case types.GzipCompressionPolicy:
		gWriter = gzip.NewWriter(pWriter)
	case types.ZlibCompressionPolicy:
		gWriter = zlib.NewWriter(pWriter)
	default:
		return nil, fmt.Errorf("unsupported Compression Policy")

	}

	go func() {
		var err error
		var n int64
		defer func() {
			if err := pWriter.CloseWithError(err); err != nil {
				logger.Error("Failed to close compressor pwriter.", zap.NamedError("error", err))
			}
		}()
		defer func() {
			if err := gWriter.Close(); err != nil {
				logger.Error("Failed to close compressor gwriter.", zap.NamedError("error", err))
			}
		}()
		defer func() {
			if err := data.Close(); err != nil {
				logger.Error("Failed to close compressor reader.", zap.NamedError("error", err))
			}
		}()
		n, err = io.Copy(gWriter, data)
		if err != nil {
			logger.Error("Compression failed.", zap.NamedError("error", err))
			return
		}
		logger.Info("compression complete.", zap.Int64("writtenBytes", n))
	}()

	return pReader, nil
}

func DecompressSnapshot(data io.ReadCloser, compressionPolicy string) (io.ReadCloser, error) {
	logger := log.NewLogger()

	var deCompressedData io.ReadCloser
	var err error

	logger.Info("Start decompressing the snapshot.", zap.String("compressionPolicy", compressionPolicy))

	switch compressionPolicy {
	case types.ZlibCompressionPolicy:
		deCompressedData, err = zlib.NewReader(data)
		if err != nil {
			logger.Error("Unable to decompress.", zap.NamedError("error", err))
			return data, err
		}

	case types.GzipCompressionPolicy:
		deCompressedData, err = gzip.NewReader(data)
		if err != nil {
			logger.Error("Unable to decompress.", zap.NamedError("error", err))
			return data, err
		}
	default:
		return data, fmt.Errorf("unsupported Compression Policy")
	}

	return deCompressedData, nil
}
