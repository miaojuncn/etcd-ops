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
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
)

type LocalStore struct {
	prefix string
}

func NewLocalStore(prefix string) (*LocalStore, error) {
	if len(prefix) != 0 {
		err := os.MkdirAll(prefix, 0700)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	return &LocalStore{
		prefix: prefix,
	}, nil
}

func (s *LocalStore) Fetch(snap types.Snapshot) (io.ReadCloser, error) {
	return os.Open(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
}

// Save will write the snapshot to local store
func (s *LocalStore) Save(snap types.Snapshot, rc io.ReadCloser) error {
	defer func() {
		if err := rc.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close reader when saving snapshot: %v", err)
		}
	}()
	err := os.MkdirAll(path.Join(s.prefix, snap.SnapDir), 0700)
	if err != nil && !os.IsExist(err) {
		return err
	}
	f, err := os.Create(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close snapshot file: %v", err)
		}
	}()
	_, err = io.Copy(f, rc)
	if err != nil {
		return err
	}
	return f.Sync()
}

// List will return sorted list with all snapshot files on store.
func (s *LocalStore) List() (types.SnapList, error) {
	snapList := types.SnapList{}
	err := filepath.Walk(s.prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			zlog.Logger.Errorf("Prevent panic by handling failure accessing a path %q: %v", path, err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		snap, err := types.ParseSnapshot(path)
		if err != nil {
			zlog.Logger.Warnf("Invalid snapshot found. Ignoring it:%s\n", path)
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
func (s *LocalStore) Delete(snap types.Snapshot) error {
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
func (s *LocalStore) Size(snap types.Snapshot) (int64, error) {
	fileInfo, err := os.Stat(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}
