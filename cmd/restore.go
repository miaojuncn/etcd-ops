package cmd

import (
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/spf13/cobra"
)

func NewRestoreCommand() *cobra.Command {
	opts := newRestoreOptions()
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "restore an etcd member data directory from snapshots",
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			if err := opts.validate(); err != nil {
				zlog.Logger.Fatalf("Failed to validate the options: %v", err)
				return
			}
			rs, err := restorer.NewRestorer(opts.restoreConfig, opts.storeConfig)
			if err != nil {
				zlog.Logger.Fatalf("Failed to create restorer: %v", err)
				return
			}
			err = rs.RestoreAndStopEtcd()
			if err != nil {
				zlog.Logger.Fatalf("Failed to restore snapshot: %v", err)
				return
			}
			zlog.Logger.Info("Restore the etcd data successfully")
		},
	}
	opts.addFlags(restoreCmd.Flags())
	return restoreCmd
}
