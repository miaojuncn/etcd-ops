package cmd

import (
	"context"
	"fmt"

	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewRestoreCommand(ctx context.Context) *cobra.Command {
	opts := newRestoreOptions()
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "restore an etcd member data directory from snapshots",
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			if err := opts.validate(); err != nil {
				zap.S().Fatalf("Failed to validate the options: %v", err)
				return
			}
			rs, err := restorer.NewRestorer(opts.restoreConfig, opts.storeConfig)
			if err != nil {
				zap.S().Fatalf("Failed to create restorer: %v", err)
				return
			}
			fmt.Println(rs)
		},
	}
	opts.addFlags(restoreCmd.Flags())
	return restoreCmd
}
