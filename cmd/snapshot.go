package cmd

import (
	"context"

	"github.com/miaojuncn/etcd-ops/pkg/log"
	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/defrag"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/snapaction"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
)

func SnapshotCommand(ctx context.Context) *cobra.Command {
	opts := newSnapshotOptions()
	var command = &cobra.Command{
		Use:   "snapshot",
		Short: "takes the snapshot of etcd periodically",
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()

			logger := log.NewLogger()

			go metricsServer(ctx, logger)
			if err := opts.validate(); err != nil {
				logger.Fatal("Failed to validate the snapshot options.", zap.NamedError("error", err))
				return
			}
			s, err := store.GetStore(opts.storeConfig)
			if err != nil {
				logger.Fatal("Failed to create store from configured storage provider.", zap.NamedError("error", err))
			}

			sa, err := snapaction.NewSnapAction(logger, opts.etcdConnectionConfig, opts.policyConfig,
				opts.compressionConfig, opts.storeConfig, s)

			if err != nil {
				logger.Fatal("Failed to create snap action.", zap.NamedError("error", err))
			}

			defragSchedule, err := cron.ParseStandard(opts.defragSchedule)
			if err != nil {
				logger.Fatal("Failed to parse defrag schedule.", zap.NamedError("error", err))
				return
			}
			go defrag.DataDefragPeriodically(ctx, opts.etcdConnectionConfig, defragSchedule, logger)

			go sa.RunGarbageCollector(ctx.Done())

			if err := sa.Run(ctx.Done()); err != nil {
				logger.Fatal("Snapshot failed.", zap.NamedError("error", err))
			}
			logger.Info("Shutting down...")
		},
	}
	opts.addFlags(command.Flags())
	return command
}
