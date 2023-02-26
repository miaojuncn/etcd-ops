package cmd

import (
	"context"

	"github.com/spf13/cobra"
)

func NewRestoreCommand(ctx context.Context) *cobra.Command {
	opts := newRestoreOptions()
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "restore an etcd member data directory from snapshots",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}
	opts.addFlags(restoreCmd.Flags())
	return restoreCmd
}
