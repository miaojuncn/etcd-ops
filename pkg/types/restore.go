package types

import (
	"fmt"
	"path"
	"time"

	flag "github.com/spf13/pflag"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
	defaultInitialClusterToken      = "etcd-cluster"
	defaultMaxFetchers              = 6
	defaultMaxCallSendMsgSize       = 10 * 1024 * 1024 // 10Mib
	defaultMaxRequestBytes          = 10 * 1024 * 1024 // 10Mib
	defaultMaxTxnOps                = 10 * 1024
	defaultEmbeddedEtcdQuotaBytes   = 8 * 1024 * 1024 * 1024 // 8Gib
	defaultAutoCompactionMode       = "periodic"             // only 2 mode is supported: 'periodic' or 'revision'
	defaultAutoCompactionRetention  = "30m"
)

type Event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

// FetcherInfo stores the information about fetcher
type FetcherInfo struct {
	Snapshot  Snapshot
	SnapIndex int
}

// ApplierInfo stores the info about applier
type ApplierInfo struct {
	EventsFilePath string
	SnapIndex      int
}

// RestoreConfig holds the restoration configuration.
type RestoreConfig struct {
	InitialCluster           string   `json:"initialCluster"`
	InitialClusterToken      string   `json:"initialClusterToken,omitempty"`
	RestoreDataDir           string   `json:"restoreDataDir,omitempty"`
	InitialAdvertisePeerURLs []string `json:"initialAdvertisePeerURLs"`
	Name                     string   `json:"name"`
	SkipHashCheck            bool     `json:"skipHashCheck,omitempty"`
	MaxFetchers              uint     `json:"maxFetchers,omitempty"`
	MaxRequestBytes          uint     `json:"MaxRequestBytes,omitempty"`
	MaxTxnOps                uint     `json:"MaxTxnOps,omitempty"`
	MaxCallSendMsgSize       int      `json:"maxCallSendMsgSize,omitempty"`
	EmbeddedEtcdQuotaBytes   int64    `json:"embeddedEtcdQuotaBytes,omitempty"`
	AutoCompactionMode       string   `json:"autoCompactionMode,omitempty"`
	AutoCompactionRetention  string   `json:"autoCompactionRetention,omitempty"`
}

// NewRestoreConfig returns the restoration config.
func NewRestoreConfig() *RestoreConfig {
	return &RestoreConfig{
		InitialCluster:           initialClusterFromName(defaultName),
		InitialClusterToken:      defaultInitialClusterToken,
		RestoreDataDir:           fmt.Sprintf("%s.etcd", defaultName),
		InitialAdvertisePeerURLs: []string{defaultInitialAdvertisePeerURLs},
		Name:                     defaultName,
		SkipHashCheck:            false,
		MaxFetchers:              defaultMaxFetchers,
		MaxCallSendMsgSize:       defaultMaxCallSendMsgSize,
		MaxRequestBytes:          defaultMaxRequestBytes,
		MaxTxnOps:                defaultMaxTxnOps,
		EmbeddedEtcdQuotaBytes:   int64(defaultEmbeddedEtcdQuotaBytes),
		AutoCompactionMode:       defaultAutoCompactionMode,
		AutoCompactionRetention:  defaultAutoCompactionRetention,
	}
}

// AddFlags adds the flags to flagSet.
func (c *RestoreConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.InitialCluster, "initial-cluster", c.InitialCluster, "initial cluster configuration for restore bootstrap")
	fs.StringVar(&c.InitialClusterToken, "initial-cluster-token", c.InitialClusterToken, "initial cluster token for the etcd cluster during restore bootstrap")
	fs.StringVarP(&c.RestoreDataDir, "data-dir", "d", c.RestoreDataDir, "path to the data directory")
	fs.StringArrayVar(&c.InitialAdvertisePeerURLs, "initial-advertise-peer-urls", c.InitialAdvertisePeerURLs, "list of this member's peer URLs to advertise to the rest of the cluster")
	fs.StringVar(&c.Name, "name", c.Name, "human-readable name for this member")
	fs.BoolVar(&c.SkipHashCheck, "skip-hash-check", c.SkipHashCheck, "ignore snapshot integrity hash value (required if copied from data directory)")
	fs.UintVar(&c.MaxFetchers, "max-fetchers", c.MaxFetchers, "maximum number of threads that will fetch delta snapshots in parallel")
	fs.IntVar(&c.MaxCallSendMsgSize, "max-call-send-message-size", c.MaxCallSendMsgSize, "maximum size of message that the client sends")
	fs.UintVar(&c.MaxRequestBytes, "max-request-bytes", c.MaxRequestBytes, "Maximum client request size in bytes the server will accept")
	fs.UintVar(&c.MaxTxnOps, "max-txn-ops", c.MaxTxnOps, "Maximum number of operations permitted in a transaction")
	fs.Int64Var(&c.EmbeddedEtcdQuotaBytes, "embedded-etcd-quota-bytes", c.EmbeddedEtcdQuotaBytes, "maximum backend quota for the embedded etcd used for applying delta snapshots")
	fs.StringVar(&c.AutoCompactionMode, "auto-compaction-mode", c.AutoCompactionMode, "mode for auto-compaction: 'periodic' for duration based retention. 'revision' for revision number based retention.")
	fs.StringVar(&c.AutoCompactionRetention, "auto-compaction-retention", c.AutoCompactionRetention, "Auto-compaction retention length.")
}

// Validate validates the config.
func (c *RestoreConfig) Validate() error {
	if _, err := types.NewURLsMap(c.InitialCluster); err != nil {
		return fmt.Errorf("failed creating url map for restore cluster: %v", err)
	}
	if _, err := types.NewURLs(c.InitialAdvertisePeerURLs); err != nil {
		return fmt.Errorf("failed parsing peers urls for restore cluster: %v", err)
	}
	if c.MaxCallSendMsgSize <= 0 {
		return fmt.Errorf("max call send message should be greater than zero")
	}
	if c.MaxFetchers <= 0 {
		return fmt.Errorf("max fetchers should be greater than zero")
	}
	if c.EmbeddedEtcdQuotaBytes <= 0 {
		return fmt.Errorf("etcd Quota size for etcd must be greater than 0")
	}
	if c.AutoCompactionMode != "periodic" && c.AutoCompactionMode != "revision" {
		return fmt.Errorf("UnSupported auto-compaction-mode")
	}
	c.RestoreDataDir = path.Clean(c.RestoreDataDir)
	return nil
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}

// DeepCopyInto copies the structure deeply from in to out.
func (c *RestoreConfig) DeepCopyInto(out *RestoreConfig) {
	*out = *c
	if c.InitialAdvertisePeerURLs != nil {
		c, out := &c.InitialAdvertisePeerURLs, &out.InitialAdvertisePeerURLs
		*out = make([]string, len(*c))
		for i, v := range *c {
			(*out)[i] = v
		}
	}
}
