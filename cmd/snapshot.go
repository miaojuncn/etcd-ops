package cmd

import (
	"context"

	"github.com/miaojuncn/etcd-ops/pkg/defragmentor"
	"github.com/miaojuncn/etcd-ops/pkg/snaptaker"
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

			st, err := snaptaker.NewSnapTaker(opts.etcdConnectionConfig, opts.policyConfig, opts.compressionConfig, opts.storeConfig, s)
			if err != nil {
				zap.S().Fatalf("failed to create snaptaker: %v", err)
			}

			defragSchedule, err := cron.ParseStandard(opts.defragmentationSchedule)
			if err != nil {
				zap.S().Fatalf("failed to parse defragmentation schedule: %v", err)
				return
			}
			go defragmentor.DefragDataPeriodically(ctx, opts.etcdConnectionConfig, defragSchedule, st.TriggerFullSnapshot)
		},
	}
	opts.addFlags(command.Flags())
	return command
}
