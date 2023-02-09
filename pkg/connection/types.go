package connection

import "time"

const (
	defaultEtcdConnectionEndpoint string        = "127.0.0.1:2379"
	DefaultEtcdConnectionTimeout  time.Duration = 30 * time.Second
	DefaultDefragTimeout          time.Duration = 8 * time.Minute
	DefaultSnapshotTimeout        time.Duration = 15 * time.Minute
	DefragRetryPeriod             time.Duration = 1 * time.Minute
)

type EtcdConnectionConfig struct {
	Endpoints          []string      `json:"endpoints"`
	ServiceEndpoints   []string      `json:"serviceEndpoints,omitempty"`
	Username           string        `json:"username,omitempty"`
	Password           string        `json:"password,omitempty"`
	ConnectionTimeout  time.Duration `json:"connectionTimeout,omitempty"`
	SnapshotTimeout    time.Duration `json:"snapshotTimeout,omitempty"`
	DefragTimeout      time.Duration `json:"defragTimeout,omitempty"`
	InsecureTransport  bool          `json:"insecureTransport,omitempty"`
	InsecureSkipVerify bool          `json:"insecureSkipVerify,omitempty"`
	CertFile           string        `json:"certFile,omitempty"`
	KeyFile            string        `json:"keyFile,omitempty"`
	CaFile             string        `json:"caFile,omitempty"`
	MaxCallSendMsgSize int           `json:"maxCallSendMsgSize,omitempty"`
}

func NewEtcdConnectionConfig() *EtcdConnectionConfig {
	return &EtcdConnectionConfig{
		Endpoints:          []string{defaultEtcdConnectionEndpoint},
		ConnectionTimeout:  DefaultEtcdConnectionTimeout,
		SnapshotTimeout:    DefaultSnapshotTimeout,
		DefragTimeout:      DefaultDefragTimeout,
		InsecureTransport:  true,
		InsecureSkipVerify: false,
	}
}
