package snapshot

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/snaptaker"
	"github.com/miaojuncn/etcd-ops/pkg/tools"
	"go.uber.org/zap"
)

func (s SnapList) Len() int      { return len(s) }
func (s SnapList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SnapList) Less(i, j int) bool {
	// Ignoring errors here.
	iLastRevision := s[i].LastRevision
	jLastRevision := s[j].LastRevision

	if iLastRevision < jLastRevision {
		return true
	}

	if iLastRevision == jLastRevision {
		if !s[i].IsChunk && s[j].IsChunk {
			return true
		}
		if s[i].IsChunk && !s[j].IsChunk {
			return false
		}
		if !s[i].IsChunk && !s[j].IsChunk {
			return s[i].CreatedOn.Unix() < s[j].CreatedOn.Unix()
		}
		// If both are chunks, ordering doesn't matter.
		return true
	}

	return false
}

// GenerateSnapshotName prepares the snapshot name from metadata
func (s *Snapshot) GenerateSnapshotName() {
	s.SnapName = fmt.Sprintf("%s-%08d-%08d-%d%s", s.Kind, s.StartRevision, s.LastRevision, s.CreatedOn.Unix(), s.CompressionSuffix)
}

// GenerateSnapshotDirectory prepares the snapshot directory name from metadata
func (s *Snapshot) GenerateSnapshotDirectory() {
	s.SnapDir = fmt.Sprintf("Backup-%s", tools.UnixTime2String(s.CreatedOn))
}

// ParseSnapshot parse <snapPath> to create snapshot structure
func ParseSnapshot(snapPath string) (*Snapshot, error) {
	zap.L().Info("parse snapshot in path", zap.String("snapPath", snapPath))
	var err error
	s := &Snapshot{}

	tok := strings.Split(snapPath, "/")
	if len(tok) < 1 || len(tok) > 5 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapPath)
	}

	snapName := tok[len(tok)-1]
	snapDir := tok[len(tok)-2]
	s.IsChunk = true

	zap.L().Info("get snapDir && snapName in path",
		zap.String("path", snapPath),
		zap.String("snapDir", snapDir),
		zap.String("snapName", snapName))

	tokens := strings.Split(snapName, "-")
	if len(tokens) != 4 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapName)
	}

	// parse kind
	switch tokens[0] {
	case snaptaker.SnapshotKindFull:
		s.Kind = snaptaker.SnapshotKindFull
	case snaptaker.SnapshotKindDelta:
		s.Kind = snaptaker.SnapshotKindDelta
	default:
		return nil, fmt.Errorf("unknown snapshot kind: %s", tokens[0])
	}

	// parse start revision
	s.StartRevision, err = strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid start revision: %s", tokens[1])
	}
	// parse last revision
	s.LastRevision, err = strconv.ParseInt(tokens[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid last revision: %s", tokens[2])
	}

	if s.StartRevision > s.LastRevision {
		return nil, fmt.Errorf("last revision (%s) should be at least start revision(%s) ", tokens[2], tokens[1])
	}

	// parse creation time as well as parse the Snapshot compression suffix
	// Kind-StartRevision-LastRevision-CreatedOn.Unix()CompressionSuffix
	timeWithSnapSuffix := strings.Split(tokens[len(tokens)-1], ".")
	if len(timeWithSnapSuffix) > 1 {
		s.CompressionSuffix = "." + timeWithSnapSuffix[1]
	}

	unixTime, err := strconv.ParseInt(timeWithSnapSuffix[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid creation time: %s", tokens[3])
	}
	s.CreatedOn = time.Unix(unixTime, 0).UTC()
	s.SnapName = snapName
	s.SnapDir = snapDir
	return s, nil
}
