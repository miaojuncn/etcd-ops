package compressor

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
