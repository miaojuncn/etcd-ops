package cmd

import (
	"context"
	"strings"

	"github.com/miaojuncn/etcd-ops/pkg/compactor"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/server/v3/mvcc"
)

// NewCompactCommand compacts the ETCD instance
func NewCompactCommand(ctx context.Context) *cobra.Command {
	opts := newCompactOptions()
	compactCmd := &cobra.Command{
		Use:   "compact",
		Short: "compacts multiple incremental snapshots in etcd backup into a single full snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			/* Compact operation
			- Restore from all the latest snapshots (Base + Delta).
			- Compact the newly created embedded ETCD instance.
			- Defrag
			- Save the snapshot
			*/
			if err := opts.validate(); err != nil {
				zlog.Logger.Fatalf("Failed to validate the options: %v", err)
				return
			}

			rs, err := restorer.NewRestorer(opts.restoreConfig, opts.storeConfig)
			if err != nil {
				return
			}

			cp := compactor.Compactor{
				Restorer:      rs,
				CompactConfig: opts.compactConfig,
			}

			snapshot, err := cp.Compact(ctx, opts.storeConfig)
			if err != nil {
				if strings.Contains(err.Error(), mvcc.ErrCompacted.Error()) {
					zlog.Logger.Warnf("Stopping backup compaction: %v", err)
				} else {
					zlog.Logger.Fatalf("Failed to compact snapshot: %v", err)
				}
				return
			}
			zlog.Logger.Infof("Compacted snapshot directory: %v, snapshot name: %v", snapshot.SnapDir, snapshot.SnapName)

		},
	}

	opts.addFlags(compactCmd.Flags())
	return compactCmd
}
