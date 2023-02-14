package compressor

import (
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/miaojuncn/etcd-ops/pkg/types"
	"go.uber.org/zap"
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
	pReader, pWriter := io.Pipe()

	var gWriter io.WriteCloser
	zap.S().Infof("start compressing the snapshot with %v compressionPolicy", compressionPolicy)

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
		defer pWriter.CloseWithError(err)
		defer gWriter.Close()
		defer data.Close()
		n, err = io.Copy(gWriter, data)
		if err != nil {
			zap.S().Errorf("compression failed: %v", err)
			return
		}
		zap.S().Infof("compression complete, total written bytes %v", n)
	}()

	return pReader, nil
}

func DecompressSnapshot(data io.ReadCloser, compressionPolicy string) (io.ReadCloser, error) {
	var deCompressedData io.ReadCloser
	var err error

	zap.S().Infof("start decompressing the snapshot with %v compressionPolicy", compressionPolicy)

	switch compressionPolicy {
	case types.ZlibCompressionPolicy:
		deCompressedData, err = zlib.NewReader(data)
		if err != nil {
			zap.S().Errorf("unable to decompress: %v", err)
			return data, err
		}

	case types.GzipCompressionPolicy:
		deCompressedData, err = gzip.NewReader(data)
		if err != nil {
			zap.S().Errorf("unable to decompress: %v", err)
			return data, err
		}
	default:
		return data, fmt.Errorf("unsupported Compression Policy")
	}

	return deCompressedData, nil
}
