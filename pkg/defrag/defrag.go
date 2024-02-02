package defrag

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/robfig/cron/v3"
)

// defragJob implement the cron.Job for etcd defrag.
type defragJob struct {
	ctx                  context.Context
	etcdConnectionConfig *types.EtcdConnectionConfig
	logger               *zap.Logger
}

// NewDefragJob returns the new defrag job.
func NewDefragJob(ctx context.Context, etcdConnectionConfig *types.EtcdConnectionConfig, logger *zap.Logger) cron.Job {
	return &defragJob{
		ctx:                  ctx,
		etcdConnectionConfig: etcdConnectionConfig,
		logger:               logger.With(zap.String("actor", "defrag")),
	}
}

func (d *defragJob) Run() {
	clientFactory := etcd.NewFactory(*d.etcdConnectionConfig)

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		d.logger.Warn("Failed to create etcd maintenance client.")
	}
	defer func() {
		if err := clientMaintenance.Close(); err != nil {
			d.logger.Error("Failed to close etcd maintenance client.", zap.NamedError("error", err))
		}
	}()

	client, err := clientFactory.NewCluster()
	if err != nil {
		d.logger.Warn("Failed to create etcd cluster client.")
	}
	defer func() {
		if err := client.Close(); err != nil {
			d.logger.Error("Failed to close etcd client.", zap.NamedError("error", err))
		}
	}()

	ticker := time.NewTicker(types.DefragRetryPeriod)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			etcdEndpoints, err := tools.GetAllEtcdEndpoints(d.ctx, client, d.etcdConnectionConfig, d.logger)
			if err != nil {
				d.logger.Error("Failed to get endpoints of all members of etcd cluster.", zap.NamedError("error", err))
				continue
			}
			d.logger.Info("Get all etcd members endpoints.", zap.Strings("endpoints", etcdEndpoints))

			isClusterHealthy, err := tools.IsEtcdClusterHealthy(d.ctx, clientMaintenance, d.etcdConnectionConfig, etcdEndpoints, d.logger)
			if err != nil {
				d.logger.Error("Failed to defrag as all members of etcd cluster are not healthy.", zap.NamedError("error", err))
				continue
			}

			if isClusterHealthy {
				d.logger.Info("Starting the defrag as all members of etcd cluster are in healthy state.")
				err = etcd.DefragData(d.ctx, clientMaintenance, client, etcdEndpoints, d.etcdConnectionConfig.DefragTimeout, d.logger)
				if err != nil {
					d.logger.Warn("Failed to defrag data.", zap.NamedError("error", err))
				}
				break waitLoop
			}
		}
	}
}

// DataDefragPeriodically defrag the data directory of each etcd member.
func DataDefragPeriodically(ctx context.Context, etcdConnectionConfig *types.EtcdConnectionConfig,
	defragSchedule cron.Schedule, logger *zap.Logger) {
	job := NewDefragJob(ctx, etcdConnectionConfig, logger)
	jobRunner := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)))
	jobRunner.Schedule(defragSchedule, job)

	jobRunner.Start()

	<-ctx.Done()
	logger.Info("Closing defrag.")
	jobRunnerCtx := jobRunner.Stop()
	<-jobRunnerCtx.Done()
}
