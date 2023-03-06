package compactor

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdDialTimeout = time.Second * 30
)

type Compactor struct {
	*restorer.Restorer
	*types.CompactConfig
}

// Compact is mainly responsible for applying snapshots (full + delta), compacting, defrag, taking the snapshot and saving it sequentially.
func (c *Compactor) Compact(ctx context.Context, store *types.StoreConfig) (*types.Snapshot, error) {
	zlog.Logger.Info("Start compacting")

	// Deep copy restore options ro to avoid any mutation of the passing object
	ro := c.Restorer.DeepCopy()

	// If no base snapshot is found, abort compaction as there would be nothing to compact
	if ro.BaseSnapshot == nil {
		zlog.Logger.Error("No base snapshot found. Nothing is available for compaction")
		return nil, fmt.Errorf("no base snapshot found. Nothing is available for compaction")
	}

	// Set a temporary etcd data directory for embedded etcd
	prefix := ro.Config.RestoreDataDir
	if prefix == "" {
		prefix = "/tmp"
	}
	compactDir, err := os.MkdirTemp(prefix, "compactor-")
	if err != nil {
		zlog.Logger.Errorf("Unable to create temporary etcd directory for compaction: %s", err.Error())
		return nil, err
	}

	defer os.RemoveAll(compactDir)

	ro.Config.RestoreDataDir = compactDir

	// Then restore from the snapshots
	r, err := restorer.NewRestorer(ro.Config, store)
	embeddedEtcd, err := r.Restore()
	if err != nil {
		return nil, fmt.Errorf("unable to restore snapshots during compaction: %v", err)
	}

	zlog.Logger.Info("Restore for compaction is over")
	// There is a possibility that restore operation may not start an embedded ETCD.
	if embeddedEtcd == nil {
		embeddedEtcd, err = restorer.StartEmbeddedEtcd(r)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		embeddedEtcd.Server.Stop()
		embeddedEtcd.Close()
	}()

	ep := []string{embeddedEtcd.Clients[0].Addr().String()}

	// Then compact ETCD
	clientFactory := etcd.NewClientFactory(ro.NewClientFactory, types.EtcdConnectionConfig{
		MaxCallSendMsgSize: ro.Config.MaxCallSendMsgSize,
		Endpoints:          ep,
		InsecureTransport:  true,
	})
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return nil, fmt.Errorf("failed to build etcd KV client")
	}
	defer clientKV.Close()

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		return nil, fmt.Errorf("failed to build etcd maintenance client")
	}
	defer clientMaintenance.Close()

	revCheckCtx, cancel := context.WithTimeout(ctx, etcdDialTimeout)
	getResponse, err := clientKV.Get(revCheckCtx, "foo")
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd KV client: %v", err)
	}
	etcdRevision := getResponse.Header.GetRevision()

	// Compact
	if _, err := clientKV.Compact(ctx, etcdRevision, clientv3.WithCompactPhysical()); err != nil {
		return nil, fmt.Errorf("failed to compact: %v", err)
	}

	// Then defrag ETCD
	if c.NeedDefrag {
		client, err := clientFactory.NewCluster()
		if err != nil {
			return nil, fmt.Errorf("failed to build etcd cluster client")
		}
		defer client.Close()

		err = etcd.DefragData(ctx, clientMaintenance, client, ep, c.DefragTimeout)
		if err != nil {
			zlog.Logger.Errorf("Failed to defrag: %v", err)
		}
	}

	// Then take snapshot of ETCD
	snapshotReqCtx, cancel := context.WithTimeout(ctx, c.SnapshotTimeout)
	defer cancel()

	// Determine suffix of compacted snapshot that will be result of this compaction
	suffix := ro.BaseSnapshot.CompressionSuffix
	if len(ro.DeltaSnapList) > 0 {
		suffix = ro.DeltaSnapList[ro.DeltaSnapList.Len()-1].CompressionSuffix
	}

	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(suffix)
	if err != nil {
		return nil, fmt.Errorf("unable to determine if snapshot is compressed: %v", ro.BaseSnapshot.CompressionSuffix)
	}

	isFinal := ro.BaseSnapshot.IsFinal

	cc := &types.CompressionConfig{Enabled: isCompressed, CompressionPolicy: compressionPolicy}
	snapshot, err := etcd.TakeAndSaveFullSnapshot(snapshotReqCtx, clientMaintenance, c.Store, etcdRevision, cc, suffix, isFinal)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}
