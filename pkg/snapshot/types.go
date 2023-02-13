package snapshot

import (
	"time"
)

const (
	FinalSuffix = ".final"
)

type Snapshot struct {
	Kind              string    `json:"kind"` //incr:incremental,full:full
	StartRevision     int64     `json:"startRevision"`
	LastRevision      int64     `json:"lastRevision"` // latest revision on snapshot
	CreatedOn         time.Time `json:"createdOn"`
	SnapDir           string    `json:"snapDir"`
	SnapName          string    `json:"snapName"`
	IsChunk           bool      `json:"isChunk"`
	CompressionSuffix string    `json:"compressionSuffix"`
	IsFinal           bool      `json:"isFinal"`
}

func NewSnapshot(kind string, startRevision, lastRevision int64, compressionSuffix string, isFinal bool) *Snapshot {
	snap := &Snapshot{
		Kind:              kind,
		StartRevision:     startRevision,
		LastRevision:      lastRevision,
		CreatedOn:         time.Now().UTC(),
		CompressionSuffix: compressionSuffix,
		IsFinal:           isFinal,
	}
	snap.GenerateSnapshotName()
	return snap
}

type SnapList []*Snapshot
