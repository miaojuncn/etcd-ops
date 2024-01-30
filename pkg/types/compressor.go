package types

import (
	"fmt"

	flag "github.com/spf13/pflag"
)

const (
	DefaultCompression       = false
	GzipCompressionPolicy    = "gzip"
	ZlibCompressionPolicy    = "zlib"
	DefaultCompressionPolicy = "gzip"

	UnCompressSnapshotExtension = ""
	GzipCompressionExtension    = ".gz"
	ZlibCompressionExtension    = ".zlib"
)

type CompressionConfig struct {
	Enabled           bool   `json:"enabled"`
	CompressionPolicy string `json:"policy,omitempty"`
}

func NewCompressorConfig() *CompressionConfig {
	return &CompressionConfig{
		Enabled:           DefaultCompression,
		CompressionPolicy: DefaultCompressionPolicy,
	}
}

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
