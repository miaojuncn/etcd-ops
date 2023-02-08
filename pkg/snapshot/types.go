package snapshot

import (
	"time"
)

const (
	SnapshotKindFull  = "Full"
	SnapshotKindDelta = "Incr"
	SnapshotKindChunk = "Chunk"
	// FinalSuffix is the suffix appended to the names of final snapshots.
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

type SnapList []*Snapshot
