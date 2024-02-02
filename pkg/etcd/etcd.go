package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/errors"
	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/metrics"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewFactory returns a Factory that constructs new clients using the supplied ETCD client configuration.
func NewFactory(cfg types.EtcdConnectionConfig, opts ...client.Option) client.Factory {
	options := &client.Options{}
	for _, opt := range opts {
		opt.ApplyTo(options)
	}

	var f = factoryImpl{
		EtcdConnectionConfig: cfg,
		options:              options,
	}

	return &f
}

// factoryImpl implements the client.Factory interface by constructing new client objects.
type factoryImpl struct {
	types.EtcdConnectionConfig
	options *client.Options
}

func (f *factoryImpl) NewClient() (*clientv3.Client, error) {
	return GetTLSClientForEtcd(&f.EtcdConnectionConfig, f.options)
}

func (f *factoryImpl) NewCluster() (client.ClusterCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewKV() (client.KVCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewMaintenance() (client.MaintenanceCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewWatcher() (clientv3.Watcher, error) {
	return f.NewClient()
}

type NewClientFactoryFunc func(cfg types.EtcdConnectionConfig, opts ...client.Option) client.Factory

// NewClientFactory returns the Factory using the supplied EtcdConnectionConfig.
func NewClientFactory(fn NewClientFactoryFunc, cfg types.EtcdConnectionConfig) client.Factory {
	if fn == nil {
		fn = NewFactory
	}
	return fn(cfg)
}

// GetTLSClientForEtcd creates an etcd client using the TLS config params.
func GetTLSClientForEtcd(tlsConfig *types.EtcdConnectionConfig, options *client.Options) (*clientv3.Client, error) {
	// set tls if any one tls option set
	var cfgTls *transport.TLSInfo
	tlsInfo := transport.TLSInfo{}
	if tlsConfig.CertFile != "" {
		tlsInfo.CertFile = tlsConfig.CertFile
		cfgTls = &tlsInfo
	}

	if tlsConfig.KeyFile != "" {
		tlsInfo.KeyFile = tlsConfig.KeyFile
		cfgTls = &tlsInfo
	}

	if tlsConfig.CaFile != "" {
		tlsInfo.TrustedCAFile = tlsConfig.CaFile
		cfgTls = &tlsInfo
	}

	endpoints := tlsConfig.Endpoints

	cfg := &clientv3.Config{
		Endpoints: endpoints,
		Context:   context.TODO(), // TODO: Use the context coming as parameter.
	}

	if cfgTls != nil {
		clientTLS, err := cfgTls.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = clientTLS
	}

	// if key/cert is not given but user wants secure connection, we
	// should still set up an empty tls configuration for gRPC to set up secure connection.
	if cfg.TLS == nil && !tlsConfig.InsecureTransport {
		cfg.TLS = &tls.Config{}
	}

	// If the user wants to skip TLS verification then we should set
	// the InsecureSkipVerify flag in tls configuration.
	if tlsConfig.InsecureSkipVerify && cfg.TLS != nil {
		cfg.TLS.InsecureSkipVerify = true
	}

	if tlsConfig.Username != "" && tlsConfig.Password != "" {
		cfg.Username = tlsConfig.Username
		cfg.Password = tlsConfig.Password
	}

	return clientv3.New(*cfg)
}

// PerformDefrag defrag the data directory of each etcd member.
func PerformDefrag(defragCtx context.Context, client client.MaintenanceCloser, endpoint string, logger *zap.Logger) error {
	var dbSizeBeforeDefrag, dbSizeAfterDefrag int64
	logger.Info("Defrag etcd member.", zap.String("endpoint", endpoint))

	if status, err := client.Status(defragCtx, endpoint); err != nil {
		logger.Warn("Failed to get status of etcd member.", zap.String("endpoint", endpoint), zap.NamedError("error", err))
	} else {
		dbSizeBeforeDefrag = status.DbSize
	}

	start := time.Now()
	if _, err := client.Defragment(defragCtx, endpoint); err != nil {
		metrics.DefragDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse, metrics.LabelEndPoint: endpoint}).Observe(time.Since(start).Seconds())
		logger.Error("Failed to defrag etcd member.", zap.String("endpoint", endpoint), zap.NamedError("error", err))
		return err
	}
	metrics.DefragDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue, metrics.LabelEndPoint: endpoint}).Observe(time.Since(start).Seconds())
	logger.Info("Finished defrag etcd member.", zap.String("endpoint", endpoint))
	// Since below request for status races with other etcd operations. So, size returned in
	// status might vary from the precise size just after defrag.
	if status, err := client.Status(defragCtx, endpoint); err != nil {
		logger.Warn("Failed to get status of etcd member.", zap.String("endpoint", endpoint), zap.NamedError("error", err))
	} else {
		dbSizeAfterDefrag = status.DbSize
		logger.Info("DB size changed for etcd member after defrag.", zap.String("endpoint", endpoint),
			zap.Int64("size Bytes before", dbSizeBeforeDefrag), zap.Int64("size Bytes after", dbSizeAfterDefrag))
	}
	return nil
}

// DefragData calls the defrag on each etcd followers endPoints
// then calls the defrag on etcd leader endPoints.
func DefragData(defragCtx context.Context, clientMaintenance client.MaintenanceCloser, clientCluster client.ClusterCloser,
	etcdEndpoints []string, defragTimeout time.Duration, logger *zap.Logger) error {
	leaderEtcdEndpoints, followerEtcdEndpoints, err := GetEtcdEndPointsSorted(defragCtx, clientMaintenance, clientCluster, etcdEndpoints, logger)
	logger.Debug("Etcd endpoints.", zap.Strings("endpoints", etcdEndpoints))
	logger.Debug("Etcd leader endpoints.", zap.Strings("endpoints", etcdEndpoints))
	logger.Debug("Etcd follower endpoints.", zap.Strings("endpoints", etcdEndpoints))
	if err != nil {
		return err
	}

	if len(followerEtcdEndpoints) > 0 {
		logger.Info("Starting the defrag on etcd followers in a rolling manner.")
	}
	// Perform the defrag on each etcd followers.
	for _, ep := range followerEtcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(defragCtx, defragTimeout)
			defer cancel()
			if err := PerformDefrag(ctx, clientMaintenance, ep, logger); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	logger.Info("Starting the defrag on etcd leader.")
	// Perform the defrag on etcd leader.
	for _, ep := range leaderEtcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(defragCtx, defragTimeout)
			defer cancel()
			if err := PerformDefrag(ctx, clientMaintenance, ep, logger); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// GetEtcdEndPointsSorted returns the etcd leaderEndpoints and etcd followerEndpoints.
func GetEtcdEndPointsSorted(ctx context.Context, clientMaintenance client.MaintenanceCloser,
	clientCluster client.ClusterCloser, etcdEndpoints []string, logger *zap.Logger) ([]string, []string, error) {
	var leaderEtcdEndpoints []string
	var followerEtcdEndpoints []string
	var endPoint string

	ctx, cancel := context.WithTimeout(ctx, types.DefaultEtcdConnectionTimeout)
	defer cancel()

	membersInfo, err := clientCluster.MemberList(ctx)
	if err != nil {
		logger.Error("Failed to get memberList of etcd.", zap.NamedError("error", err))
		return nil, nil, err
	}

	// to handle the single node etcd case (particularly: single node embedded etcd case)
	if len(membersInfo.Members) == 1 {
		leaderEtcdEndpoints = append(leaderEtcdEndpoints, etcdEndpoints...)
		return leaderEtcdEndpoints, nil, nil
	}

	if len(etcdEndpoints) > 0 {
		endPoint = etcdEndpoints[0]
	} else {
		return nil, nil, &errors.EtcdError{
			Message: fmt.Sprintf("etcd endpoints are not passed correctly"),
		}
	}

	response, err := clientMaintenance.Status(ctx, endPoint)
	if err != nil {
		logger.Error("Failed to get status of etcd endpoint.", zap.String("endpoint", endPoint), zap.NamedError("error", err))
		return nil, nil, err
	}

	for _, member := range membersInfo.Members {
		if member.GetID() == response.Leader {
			leaderEtcdEndpoints = append(leaderEtcdEndpoints, member.GetClientURLs()...)
		} else {
			followerEtcdEndpoints = append(followerEtcdEndpoints, member.GetClientURLs()...)
		}
	}

	return leaderEtcdEndpoints, followerEtcdEndpoints, nil
}

// TakeAndSaveFullSnapshot takes full snapshot and save it to store
func TakeAndSaveFullSnapshot(ctx context.Context, client client.MaintenanceCloser, store types.Store,
	lastRevision int64, cc *types.CompressionConfig, suffix string, logger *zap.Logger) (*types.Snapshot, error) {
	startTime := time.Now()
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return nil, &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	timeTaken := time.Since(startTime)
	logger.Info("Total time taken by Snapshot API.", zap.Duration("timeTaken", timeTaken))

	if cc.Enabled {
		startTimeCompression := time.Now()
		rc, err = compressor.CompressSnapshot(rc, cc.CompressionPolicy)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain reader for compressed file: %v", err)
		}
		timeTakenCompression := time.Since(startTimeCompression)
		logger.Info("Total time taken in full snapshot compression.", zap.Duration("timeTaken", timeTakenCompression))
	}
	defer func() {
		if err := rc.Close(); err != nil {
			logger.Error("Failed to close snapshot reader.", zap.NamedError("error", err))
		}
	}()

	logger.Info("Successfully opened snapshot reader on etcd.")

	snapshot := types.NewSnapshot(types.SnapshotKindFull, 0, lastRevision, suffix)
	if err := store.Save(*snapshot, rc); err != nil {
		timeTaken = time.Since(startTime)
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: types.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken.Seconds())
		return nil, &errors.StoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}

	timeTaken = time.Since(startTime)
	logger.Info("Total time to save full snapshot.", zap.Duration("timeTaken", timeTaken))

	return snapshot, nil
}
