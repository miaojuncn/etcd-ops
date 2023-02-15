package cmd

import (
	"context"

	"github.com/miaojuncn/etcd-ops/pkg/defrag"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/snapaction"
	"github.com/miaojuncn/etcd-ops/pkg/store"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func SnapshotCommand(ctx context.Context) *cobra.Command {
	opts := newSnapshotOptions()
	var command = &cobra.Command{
		Use:   "snapshot",
		Short: "takes the snapshot of etcd periodically",
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			if err := opts.validate(); err != nil {
				zap.S().Fatalf("failed to validate the options: %v", err)
				return
			}
			s, err := store.GetStore(opts.storeConfig)
			if err != nil {
				zap.S().Fatalf("failed to create store from configured storage provider: %v", err)
			}

			sa, err := snapaction.NewSnapAction(opts.etcdConnectionConfig, opts.policyConfig, opts.compressionConfig, opts.storeConfig, s)
			if err != nil {
				zap.S().Fatalf("failed to create snap action: %v", err)
			}

			defragSchedule, err := cron.ParseStandard(opts.defragSchedule)
			if err != nil {
				zap.S().Fatalf("failed to parse defrag schedule: %v", err)
				return
			}
			go defrag.DefragDataPeriodically(ctx, opts.etcdConnectionConfig, defragSchedule, sa.TriggerFullSnapshot)

			if err := sa.Run(ctx.Done(), true); err != nil {
				zap.S().Fatalf("snapshot failed with error: %v", err)
			}
			zap.S().Info("shutting down...")
		},
	}
	opts.addFlags(command.Flags())
	return command
}
