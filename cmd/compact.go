package cmd

import (
	"context"
	"strings"

	"github.com/miaojuncn/etcd-ops/pkg/log"
	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/compactor"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
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

			logger := log.NewLogger()
			/* Compact operation
			- Restore from all the latest snapshots (Base + Delta).
			- Compact the newly created embedded ETCD instance.
			- Defrag
			- Save the snapshot
			*/
			if err := opts.validate(); err != nil {
				logger.Fatal("Failed to validate the compact options.", zap.NamedError("error", err))
				return
			}

			rs, err := restorer.NewRestorer(logger, opts.restoreConfig, opts.storeConfig)
			if err != nil {
				return
			}

			cp := compactor.Compactor{
				Logger:        logger.With(zap.String("actor", "compact")),
				Restorer:      rs,
				CompactConfig: opts.compactConfig,
			}

			snapshot, err := cp.Compact(ctx)
			if err != nil {
				if strings.Contains(err.Error(), mvcc.ErrCompacted.Error()) {
					logger.Warn("Stopping backup compaction.", zap.NamedError("error", err))
				} else {
					logger.Fatal("Failed to compact snapshot.", zap.NamedError("error", err))
				}
				return
			}
			logger.Info("Compact snapshot successfully.", zap.String("dir", snapshot.SnapDir), zap.String("filename", snapshot.SnapName))

		},
	}

	opts.addFlags(compactCmd.Flags())
	return compactCmd
}
