package cmd

import (
	"context"

	"github.com/spf13/cobra"
)

// RootCommand represents the base command when called without any subcommands
func RootCommand(ctx context.Context) *cobra.Command {
	var RootCmd = &cobra.Command{
		Use:   "etcd-ops",
		Short: "command line utility for etcd",
		Run: func(cmd *cobra.Command, args []string) {
			if version {
				printVersionInfo()
			}
		},
	}
	RootCmd.Flags().BoolVarP(&version, "version", "v", false, "print version info")
	RootCmd.AddCommand(SnapshotCommand(ctx))
	// 	NewRestoreCommand(ctx),
	// 	NewCompactCommand(ctx),
	// 	NewInitializeCommand(ctx),
	// 	NewServerCommand(ctx),
	// 	NewCopyCommand(ctx))
	return RootCmd
}
