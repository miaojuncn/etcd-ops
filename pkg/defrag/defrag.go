package defrag

import (
	"context"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/robfig/cron/v3"
)

// CallbackFunc is type declaration for callback function for defrag
type CallbackFunc func(ctx context.Context, isFinal bool) (*types.Snapshot, error)

// defragJob implement the cron.Job for etcd defrag.
type defragJob struct {
	ctx                  context.Context
	etcdConnectionConfig *types.EtcdConnectionConfig
	callback             CallbackFunc
}

// NewDefragJob returns the new defrag job.
func NewDefragJob(ctx context.Context, etcdConnectionConfig *types.EtcdConnectionConfig) cron.Job {
	return &defragJob{
		ctx:                  ctx,
		etcdConnectionConfig: etcdConnectionConfig,
	}
}

func (d *defragJob) Run() {
	clientFactory := etcd.NewFactory(*d.etcdConnectionConfig)

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		zlog.Logger.Warn("Failed to create etcd maintenance client")
	}
	defer clientMaintenance.Close()

	client, err := clientFactory.NewCluster()
	if err != nil {
		zlog.Logger.Warn("Failed to create etcd cluster client")
	}
	defer client.Close()

	ticker := time.NewTicker(types.DefragRetryPeriod)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			etcdEndpoints, err := tools.GetAllEtcdEndpoints(d.ctx, client, d.etcdConnectionConfig)
			if err != nil {
				zlog.Logger.Errorf("Failed to get endpoints of all members of etcd cluster: %v", err)
				continue
			}
			zlog.Logger.Infof("All etcd members endPoints: %v", etcdEndpoints)

			isClusterHealthy, err := tools.IsEtcdClusterHealthy(d.ctx, clientMaintenance, d.etcdConnectionConfig, etcdEndpoints)
			if err != nil {
				zlog.Logger.Errorf("Failed to defrag as all members of etcd cluster are not healthy: %v", err)
				continue
			}

			if isClusterHealthy {
				zlog.Logger.Info("Starting the defrag as all members of etcd cluster are in healthy state")
				err = etcd.DefragData(d.ctx, clientMaintenance, client, etcdEndpoints, d.etcdConnectionConfig.DefragTimeout)
				if err != nil {
					zlog.Logger.Warnf("Failed to defrag data with error: %v", err)
				}
				break waitLoop
			}
		}
	}
}

// DefragDataPeriodically defrag the data directory of each etcd member.
func DefragDataPeriodically(ctx context.Context, etcdConnectionConfig *types.EtcdConnectionConfig,
	defragSchedule cron.Schedule) {
	job := NewDefragJob(ctx, etcdConnectionConfig)
	jobRunner := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)))
	jobRunner.Schedule(defragSchedule, job)

	jobRunner.Start()

	<-ctx.Done()
	zlog.Logger.Info("Closing defrag.")
	jobRunnerCtx := jobRunner.Stop()
	<-jobRunnerCtx.Done()
}
