package cmd

import (
	"context"
	"fmt"

	"github.com/miaojuncn/etcd-ops/pkg/store"
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
			fmt.Println(s)
		},
	}
	opts.addFlags(command.Flags())
	return command
}
