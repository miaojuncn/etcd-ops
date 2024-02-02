package cmd

import (
	"github.com/miaojuncn/etcd-ops/pkg/log"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot/restorer"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewRestoreCommand() *cobra.Command {
	opts := newRestoreOptions()
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "restore an etcd member data directory from snapshots",
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()

			logger := log.NewLogger()

			if err := opts.validate(); err != nil {
				logger.Fatal("Failed to validate the restore options.", zap.NamedError("error", err))
				return
			}
			rs, err := restorer.NewRestorer(logger, opts.restoreConfig, opts.storeConfig)
			if err != nil {
				logger.Fatal("Failed to create restorer.", zap.NamedError("error", err))
				return
			}
			err = rs.RestoreAndStopEtcd()
			if err != nil {
				logger.Fatal("Failed to restore snapshot.", zap.NamedError("error", err))
				return
			}
			logger.Info("Restore the etcd data successfully.")
		},
	}
	opts.addFlags(restoreCmd.Flags())
	return restoreCmd
}
