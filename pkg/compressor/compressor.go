package compressor

import (
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
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
	zlog.Logger.Infof("start compressing the snapshot with %v compressionPolicy", compressionPolicy)

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
				zlog.Logger.Errorf("Failed to close compressor pwriter: %v", err)
			}
		}()
		defer func() {
			if err := gWriter.Close(); err != nil {
				zlog.Logger.Errorf("Failed to close compressor gwriter: %v", err)
			}
		}()
		defer func() {
			if err := data.Close(); err != nil {
				zlog.Logger.Errorf("Failed to close compressor reader: %v", err)
			}
		}()
		n, err = io.Copy(gWriter, data)
		if err != nil {
			zlog.Logger.Errorf("compression failed: %v", err)
			return
		}
		zlog.Logger.Infof("compression complete, total written bytes %v", n)
	}()

	return pReader, nil
}

func DecompressSnapshot(data io.ReadCloser, compressionPolicy string) (io.ReadCloser, error) {
	var deCompressedData io.ReadCloser
	var err error

	zlog.Logger.Infof("start decompressing the snapshot with %v compressionPolicy", compressionPolicy)

	switch compressionPolicy {
	case types.ZlibCompressionPolicy:
		deCompressedData, err = zlib.NewReader(data)
		if err != nil {
			zlog.Logger.Errorf("unable to decompress: %v", err)
			return data, err
		}

	case types.GzipCompressionPolicy:
		deCompressedData, err = gzip.NewReader(data)
		if err != nil {
			zlog.Logger.Errorf("unable to decompress: %v", err)
			return data, err
		}
	default:
		return data, fmt.Errorf("unsupported Compression Policy")
	}

	return deCompressedData, nil
}
