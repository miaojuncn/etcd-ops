package defragmentor

import (
	"context"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/etcd"
	"github.com/miaojuncn/etcd-ops/pkg/snapshot"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"go.uber.org/zap"

	cron "github.com/robfig/cron/v3"
)

// CallbackFunc is type decalration for callback function for defragmentor
type CallbackFunc func(ctx context.Context, isFinal bool) (*snapshot.Snapshot, error)

// defragmentorJob implement the cron.Job for etcd defragmentation.
type defragmentorJob struct {
	ctx                  context.Context
	etcdConnectionConfig *etcd.EtcdConnectionConfig
	callback             CallbackFunc
}

// NewDefragmentorJob returns the new defragmentor job.
func NewDefragmentorJob(ctx context.Context, etcdConnectionConfig *etcd.EtcdConnectionConfig, callback CallbackFunc) cron.Job {
	return &defragmentorJob{
		ctx:                  ctx,
		etcdConnectionConfig: etcdConnectionConfig,
		callback:             callback,
	}
}

func (d *defragmentorJob) Run() {
	clientFactory := etcd.NewFactory(*d.etcdConnectionConfig)

	clientMaintenance, err := clientFactory.NewMaintenance()
	if err != nil {
		zap.S().Warn("failed to create etcd maintenance client")
	}
	defer clientMaintenance.Close()

	client, err := clientFactory.NewCluster()
	if err != nil {
		zap.S().Warn("failed to create etcd cluster client")
	}
	defer client.Close()

	ticker := time.NewTicker(etcd.DefragRetryPeriod)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			etcdEndpoints, err := tools.GetAllEtcdEndpoints(d.ctx, client, d.etcdConnectionConfig)
			if err != nil {
				zap.S().Errorf("failed to get endpoints of all members of etcd cluster: %v", err)
				continue
			}
			zap.S().Infof("All etcd members endPoints: %v", etcdEndpoints)

			isClusterHealthy, err := tools.IsEtcdClusterHealthy(d.ctx, clientMaintenance, d.etcdConnectionConfig, etcdEndpoints)
			if err != nil {
				zap.S().Errorf("failed to defrag as all members of etcd cluster are not healthy: %v", err)
				continue
			}

			if isClusterHealthy {
				zap.S().Info("Starting the defragmentation as all members of etcd cluster are in healthy state")
				err = etcd.DefragmentData(d.ctx, clientMaintenance, client, etcdEndpoints, d.etcdConnectionConfig.DefragTimeout)
				if err != nil {
					zap.S().Warnf("failed to defrag data with error: %v", err)
				} else {
					if d.callback != nil {
						if _, err = d.callback(d.ctx, false); err != nil {
							zap.S().Warnf("defragmentation callback failed with error: %v", err)
						}
					}
					break waitLoop
				}
			}
		}
	}

}

// DefragDataPeriodically defragments the data directory of each etcd member.
func DefragDataPeriodically(ctx context.Context, etcdConnectionConfig *etcd.EtcdConnectionConfig, defragmentationSchedule cron.Schedule, callback CallbackFunc) {
	defragmentorJob := NewDefragmentorJob(ctx, etcdConnectionConfig, callback)
	jobRunner := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)))
	jobRunner.Schedule(defragmentationSchedule, defragmentorJob)

	jobRunner.Start()

	<-ctx.Done()
	zap.S().Info("Closing defragmentor.")
	jobRunnerCtx := jobRunner.Stop()
	<-jobRunnerCtx.Done()
}
