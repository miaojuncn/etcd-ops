package store

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/miaojuncn/etcd-ops/pkg/types"
	"go.uber.org/zap"
)

type LocalSnapStore struct {
	prefix string
}

func NewLocalSnapStore(prefix string) (*LocalSnapStore, error) {
	if len(prefix) != 0 {
		err := os.MkdirAll(prefix, 0700)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	return &LocalSnapStore{
		prefix: prefix,
	}, nil
}

func (s *LocalSnapStore) Fetch(snap types.Snapshot) (io.ReadCloser, error) {
	return os.Open(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
}

// Save will write the snapshot to store
func (s *LocalSnapStore) Save(snap types.Snapshot, rc io.ReadCloser) error {
	defer rc.Close()
	err := os.MkdirAll(path.Join(s.prefix, snap.SnapDir), 0700)
	if err != nil && !os.IsExist(err) {
		return err
	}
	f, err := os.Create(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, rc)
	if err != nil {
		return err
	}
	return f.Sync()
}

// List will return sorted list with all snapshot files on store.
func (s *LocalSnapStore) List() (types.SnapList, error) {
	snapList := types.SnapList{}
	err := filepath.Walk(s.prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			zap.S().Errorf("prevent panic by handling failure accessing a path %q: %v", path, err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		snap, err := types.ParseSnapshot(path)
		if err != nil {
			// Warning
			zap.S().Warnf("Invalid snapshot found. Ignoring it:%s\n", path)
		} else {
			snapList = append(snapList, snap)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking the path %q: %v", s.prefix, err)
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *LocalSnapStore) Delete(snap types.Snapshot) error {
	if err := os.Remove(path.Join(s.prefix, snap.SnapDir, snap.SnapName)); err != nil {
		return err
	}
	err := os.Remove(path.Join(s.prefix, snap.SnapDir))
	if pathErr, ok := err.(*os.PathError); ok && pathErr.Err != syscall.ENOTEMPTY {
		return err
	}
	return nil
}

// Size should return size of the snapshot file from store
func (s *LocalSnapStore) Size(snap types.Snapshot) (int64, error) {
	fileInfo, err := os.Stat(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}
