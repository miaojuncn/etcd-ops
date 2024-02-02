package compactor

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/compressor"
	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Compactor struct {
	Logger *zap.Logger
	*restorer.Restorer
	*types.CompactConfig
}

// Compact is mainly responsible for applying snapshots (full + delta), compacting, defrag, taking the snapshot and saving it sequentially.
func (c *Compactor) Compact(ctx context.Context) (*types.Snapshot, error) {
	c.Logger.Info("Start compacting.")

	// Deep copy restore options ro to avoid any mutation of the passing object
	ro := c.Restorer.DeepCopy()

	// If no base snapshot is found, abort compaction as there would be nothing to compact
	if ro.BaseSnapshot == nil {
		c.Logger.Error("No base snapshot found, nothing is available for compaction.")
		return nil, fmt.Errorf("no base snapshot found. Nothing is available for compaction")
	}

	c.Logger.Info("Creating temporary etcd directory for restore.", zap.String("dir", ro.Config.DataDir))
	err := os.MkdirAll(ro.Config.DataDir, 0700)
	if err != nil {
		c.Logger.Error("Unable to create temporary etcd directory for compaction.", zap.NamedError("error", err))
	}

	defer func() {
		if err := os.RemoveAll(ro.Config.DataDir); err != nil {
			c.Logger.Error("Failed to remove temporary etcd directory.",
				zap.String("dir", ro.Config.DataDir), zap.NamedError("error", err))
		}
	}()

	//// Then restore from the snapshots
	//r, err := restorer.NewRestorer(ro.Config, store)
	//if err != nil {
	//	return nil, err
	//}

	embeddedEtcd, err := ro.Restore()
	if err != nil {
		return nil, fmt.Errorf("unable to restore snapshots during compaction: %v", err)
	}

	c.Logger.Info("Restore for compaction is done.")
	// There is a possibility that restore operation may not start an embedded ETCD.
	if embeddedEtcd == nil {
		embeddedEtcd, err = restorer.StartEmbeddedEtcd(ro)
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
		// MaxCallSendMsgSize: ro.Config.MaxCallSendMsgSize,
		Endpoints:         ep,
		InsecureTransport: true,
	})
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return nil, fmt.Errorf("failed to build etcd KV client")
	}
	defer func() {
		if err = clientKV.Close(); err != nil {
			c.Logger.Error("Failed to close etcd KV client.", zap.NamedError("error", err))
		}
	}()

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		return nil, fmt.Errorf("failed to build etcd maintenance client")
	}
	defer func() {
		if err = clientMaintenance.Close(); err != nil {
			c.Logger.Error("Failed to close etcd maintenance client.", zap.NamedError("error", err))
		}
	}()

	revCheckCtx, cancel := context.WithTimeout(ctx, types.DefaultEtcdConnectionTimeout)
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
		defer func() {
			if err = client.Close(); err != nil {
				c.Logger.Error("failed to close etcd cluster client.", zap.NamedError("error", err))
			}
		}()

		err = etcd.DefragData(ctx, clientMaintenance, client, ep, c.DefragTimeout, c.Logger)
		if err != nil {
			c.Logger.Error("Failed to defrag.", zap.NamedError("error", err))
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

	cc := &types.CompressionConfig{Enabled: isCompressed, CompressionPolicy: compressionPolicy}
	snapshot, err := etcd.TakeAndSaveFullSnapshot(snapshotReqCtx, clientMaintenance, c.Store, etcdRevision, cc, suffix, c.Logger)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}
