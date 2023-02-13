package compressor

import (
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"

	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

// AddFlags adds the flags to flagSet.
func (c *CompressionConfig) AddFlags(fs *flag.FlagSet) {

	fs.BoolVar(&c.Enabled, "enable-compression", c.Enabled, "whether to compress the snapshots or not")
	fs.StringVar(&c.CompressionPolicy, "compression-policy", c.CompressionPolicy, "Policy for compressing the snapshots")
}

// Validate validates the compression Config.
func (c *CompressionConfig) Validate() error {

	if c.Enabled == false {
		return nil
	}

	for _, policy := range []string{GzipCompressionPolicy, ZlibCompressionPolicy} {
		if c.CompressionPolicy == policy {
			return nil
		}
	}
	return fmt.Errorf("%s: Compression Policy is not supported", c.CompressionPolicy)

}

func GetCompressionSuffix(compressionEnabled bool, compressionPolicy string) (string, error) {

	if !compressionEnabled {
		return UnCompressSnapshotExtension, nil
	}

	switch compressionPolicy {
	case ZlibCompressionPolicy:
		return ZlibCompressionExtension, nil

	case GzipCompressionPolicy:
		return GzipCompressionExtension, nil

	default:
		return "", fmt.Errorf("unsupported Compression Policy")
	}
}

func IsSnapshotCompressed(compressionSuffix string) (bool, string, error) {

	switch compressionSuffix {
	case ZlibCompressionExtension:
		return true, ZlibCompressionPolicy, nil

	case GzipCompressionExtension:
		return true, GzipCompressionPolicy, nil

	case UnCompressSnapshotExtension:
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
	case GzipCompressionPolicy:
		gWriter = gzip.NewWriter(pWriter)
	case ZlibCompressionPolicy:
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
	case ZlibCompressionPolicy:
		deCompressedData, err = zlib.NewReader(data)
		if err != nil {
			zap.S().Errorf("unable to decompress: %v", err)
			return data, err
		}

	case GzipCompressionPolicy:
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
