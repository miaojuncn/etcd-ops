package types

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/zlog"
)

type Snapshot struct {
	Kind              string    `json:"kind"` //incr:incremental,full:full
	StartRevision     int64     `json:"startRevision"`
	LastRevision      int64     `json:"lastRevision"` // latest revision on snapshot
	CreatedOn         time.Time `json:"createdOn"`
	SnapDir           string    `json:"snapDir"`
	SnapName          string    `json:"snapName"`
	CompressionSuffix string    `json:"compressionSuffix"`
}

func NewSnapshot(kind string, startRevision, lastRevision int64, compressionSuffix string) *Snapshot {
	snap := &Snapshot{
		Kind:              kind,
		StartRevision:     startRevision,
		LastRevision:      lastRevision,
		CreatedOn:         time.Now().UTC(),
		CompressionSuffix: compressionSuffix,
	}
	snap.GenerateSnapshotName()
	snap.GenerateSnapshotDirectory()
	return snap
}

type SnapList []*Snapshot

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
		return s[i].CreatedOn.Unix() < s[j].CreatedOn.Unix()
	}

	return false
}

// GenerateSnapshotName prepares the snapshot name from metadata
func (s *Snapshot) GenerateSnapshotName() {
	s.SnapName = fmt.Sprintf("%s-%08d-%08d-%d%s", s.Kind, s.StartRevision, s.LastRevision, s.CreatedOn.Unix(), s.CompressionSuffix)
}

// GenerateSnapshotDirectory prepares the snapshot directory name from metadata
func (s *Snapshot) GenerateSnapshotDirectory() {
	s.SnapDir = fmt.Sprintf("Backup-%s", s.CreatedOn.Format("20060102"))
}

// ParseSnapshot parse <snapPath> to create snapshot structure
func ParseSnapshot(snapPath string) (*Snapshot, error) {
	zlog.Logger.Debugf("Parse snapshot in path %s", snapPath)
	var err error
	s := &Snapshot{}

	tok := strings.Split(snapPath, "/")
	if len(tok) < 1 || len(tok) > 5 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapPath)
	}

	snapName := tok[len(tok)-1]
	snapDir := tok[len(tok)-2]

	zlog.Logger.Debugf("Snap Directory: %s, Snap Name: %s", snapDir, snapName)

	tokens := strings.Split(snapName, "-")
	if len(tokens) != 4 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapName)
	}

	// parse kind
	switch tokens[0] {
	case SnapshotKindFull:
		s.Kind = SnapshotKindFull
	case SnapshotKindDelta:
		s.Kind = SnapshotKindDelta
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
	if len(timeWithSnapSuffix) > 2 {
		return nil, fmt.Errorf("invalid snapshot file suffix: %v", tokens[len(tokens)-1])
	} else if len(timeWithSnapSuffix) == 2 {
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
