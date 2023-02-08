package snapshot

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/miaojuncn/etcd-ops/pkg/tools"
)

type Snapshot struct {
	Kind              string    `json:"kind"` //incr:incremental,full:full
	StartRevision     int64     `json:"startRevision"`
	LastRevision      int64     `json:"lastRevision"` // latest revision on snapshot
	CreatedOn         time.Time `json:"createdOn"`
	SnapDir           string    `json:"snapDir"`
	SnapName          string    `json:"snapName"`
	IsChunk           bool      `json:"isChunk"`
	Prefix            string    `json:"prefix"`
	CompressionSuffix string    `json:"compressionSuffix"` // CompressionSuffix depends on compessionPolicy
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

func (s *Snapshot) GetSnapshotDirectoryCreationTimeInUnix() (int64, error) {
	tok := strings.TrimPrefix(s.SnapDir, "Backup-")
	return strconv.ParseInt(tok, 10, 64)
}

// GenerateSnapshotDirectory prepares the snapshot directory name from metadata
func (s *Snapshot) GenerateSnapshotDirectory() {
	s.SnapDir = fmt.Sprintf("Backup-%s", tools.Unix2String(s.CreatedOn))
}
