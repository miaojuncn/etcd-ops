package tools

import (
	"context"
	errored "errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/miaojuncn/etcd-ops/pkg/errors"
	"github.com/miaojuncn/etcd-ops/pkg/etcd/client"
	"github.com/miaojuncn/etcd-ops/pkg/metrics"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"gopkg.in/yaml.v3"
)

const (
	// EtcdConfigFilePath is the file path where the etcd config map is mounted.
	EtcdConfigFilePath string = "/var/etcd/config/etcd.conf.yaml"
)

// GetLatestFullSnapshotAndDeltaSnapList returns the latest snapshot
func GetLatestFullSnapshotAndDeltaSnapList(store types.Store) (*types.Snapshot, types.SnapList, error) {
	var (
		fullSnapshot  *types.Snapshot
		deltaSnapList types.SnapList
	)
	snapList, err := store.List()
	if err != nil {
		return nil, nil, err
	}

	for index := len(snapList); index > 0; index-- {
		if snapList[index-1].Kind == types.SnapshotKindFull {
			fullSnapshot = snapList[index-1]
			break
		}
		deltaSnapList = append(deltaSnapList, snapList[index-1])
	}

	sort.Sort(deltaSnapList)
	if len(deltaSnapList) == 0 {
		metrics.StoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(0)
	} else {
		revisionDiff := deltaSnapList[len(deltaSnapList)-1].LastRevision - deltaSnapList[0].StartRevision
		metrics.StoreLatestDeltasRevisionsTotal.With(prometheus.Labels{}).Set(float64(revisionDiff))
	}
	return fullSnapshot, deltaSnapList, nil
}

// GetAllEtcdEndpoints returns the endPoints of all etcd-member.
func GetAllEtcdEndpoints(ctx context.Context, client client.ClusterCloser, etcdConnectionConfig *types.EtcdConnectionConfig) ([]string, error) {
	var etcdEndpoints []string

	ctx, cancel := context.WithTimeout(ctx, etcdConnectionConfig.ConnectionTimeout)
	defer cancel()

	membersInfo, err := client.MemberList(ctx)
	if err != nil {
		zlog.Logger.Errorf("Failed to get memberList of etcd with error: %v", err)
		return nil, err
	}

	for _, member := range membersInfo.Members {
		etcdEndpoints = append(etcdEndpoints, member.GetClientURLs()...)
	}

	return etcdEndpoints, nil
}

// IsEtcdClusterHealthy checks whether all members of etcd cluster are in healthy state or not.
func IsEtcdClusterHealthy(ctx context.Context, client client.MaintenanceCloser, etcdConnectionConfig *types.EtcdConnectionConfig, etcdEndpoints []string) (bool, error) {

	for _, endPoint := range etcdEndpoints {
		if err := func() error {
			ctx, cancel := context.WithTimeout(ctx, etcdConnectionConfig.ConnectionTimeout)
			defer cancel()
			if _, err := client.Status(ctx, endPoint); err != nil {
				zlog.Logger.Errorf("Failed to get status of etcd endPoint: %v with error: %v", endPoint, err)
				return err
			}
			return nil
		}(); err != nil {
			return false, err
		}
	}

	return true, nil
}

// GetEnvVarOrError returns the value of specified environment variable or error if it's not defined.
func GetEnvVarOrError(varName string) (string, error) {
	value := os.Getenv(varName)
	if value == "" {
		err := fmt.Errorf("missing environment variable %s", varName)
		return value, err
	}

	return value, nil
}

// GetConfigFilePath returns the path of the etcd configuration file
func GetConfigFilePath() string {
	// (For testing purpose) If no ETCD_CONF variable set as environment variable, then consider backup-restore server is not used for tests.
	// For tests or to run backup-restore server as standalone, user needs to set ETCD_CONF variable with proper location of ETCD config yaml
	etcdConfigForTest := os.Getenv("ETCD_CONF")
	if etcdConfigForTest != "" {
		return etcdConfigForTest
	}
	return EtcdConfigFilePath
}

// ProbeEtcd probes the etcd endpoint to check if an etcd is available
func ProbeEtcd(ctx context.Context, clientFactory client.Factory) error {
	clientKV, err := clientFactory.NewKV()
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("Failed to create etcd KV client: %v", err),
		}
	}
	defer func() {
		if err = clientKV.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close etcd KV client: %v", err)
		}
	}()

	if _, err := clientKV.Get(ctx, "foo"); err != nil {
		zlog.Logger.Errorf("Failed to connect to etcd KV client: %v", err)
		return err
	}
	return nil
}

// ReadConfigFileAsMap reads the config file given a path and converts it into a map[string]interface{}
func ReadConfigFileAsMap(path string) (map[string]interface{}, error) {
	configYML, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to read etcd config file at path: %s : %v", path, err)
	}

	c := map[string]interface{}{}
	if err := yaml.Unmarshal(configYML, &c); err != nil {
		return nil, fmt.Errorf("unable to unmarshal etcd config yaml file at path: %s : %v", path, err)
	}
	return c, nil
}

// ParsePeerURL forms a PeerURL, given podName by parsing the initial-advertise-peer-urls
func ParsePeerURL(initialAdvertisePeerURLs, podName string) (string, error) {
	tokens := strings.Split(initialAdvertisePeerURLs, "@")
	if len(tokens) < 4 {
		return "", fmt.Errorf("invalid peer URL : %s", initialAdvertisePeerURLs)
	}
	domainName := fmt.Sprintf("%s.%s.%s", tokens[1], tokens[2], "svc")
	return fmt.Sprintf("%s://%s.%s:%s", tokens[0], podName, domainName, tokens[3]), nil
}

// DoPromoteMember promotes a given learner to a voting member.
func DoPromoteMember(ctx context.Context, member *etcdserverpb.Member, cli client.ClusterCloser) error {
	memPromoteCtx, cancel := context.WithTimeout(ctx, types.DefaultEtcdConnectionTimeout)
	defer cancel()

	// Member promote call will succeed only if member is in sync with leader, and will error out otherwise
	_, err := cli.MemberPromote(memPromoteCtx, member.ID)
	if err == nil {
		// Member successfully promoted
		zlog.Logger.Infof("Member %v with [ID: %v] has been promoted", member.GetName(), strconv.FormatUint(member.GetID(), 16))
		return nil
	} else if errored.Is(err, rpctypes.Error(rpctypes.ErrGRPCMemberNotLearner)) {
		// Member is not a learner
		zlog.Logger.Info("Member ", member.Name, " : ", member.ID, " already a voting member of cluster.")
		return nil
	}

	return err
}

// RemoveMemberFromCluster removes member of given ID from etcd cluster.
func RemoveMemberFromCluster(ctx context.Context, cli client.ClusterCloser, memberID uint64) error {
	_, err := cli.MemberRemove(ctx, memberID)
	if err != nil {
		return fmt.Errorf("unable to remove member [ID:%v] from the cluster: %v", strconv.FormatUint(memberID, 16), err)
	}

	zlog.Logger.Infof("successfully removed member [ID: %v] from the cluster", strconv.FormatUint(memberID, 16))
	return nil
}

// CheckIfLearnerPresent checks whether a learner(non-voting) member present or not.
func CheckIfLearnerPresent(ctx context.Context, cli client.ClusterCloser) (bool, error) {
	membersInfo, err := cli.MemberList(ctx)
	if err != nil {
		return false, fmt.Errorf("error listing members: %v", err)
	}

	for _, member := range membersInfo.Members {
		if member.IsLearner {
			return true, nil
		}
	}
	return false, nil
}
