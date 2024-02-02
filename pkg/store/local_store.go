package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/miaojuncn/etcd-ops/pkg/log"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"go.uber.org/zap"
)

type LocalStore struct {
	prefix string
	logger *zap.Logger
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
		logger: log.NewLogger().With(zap.String("actor", "store")),
	}, nil
}

func (s *LocalStore) Fetch(snap types.Snapshot) (io.ReadCloser, error) {
	return os.Open(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
}

// Save will write the snapshot to local store
func (s *LocalStore) Save(snap types.Snapshot, rc io.ReadCloser) error {
	defer func() {
		if err := rc.Close(); err != nil {
			s.logger.Error("Failed to close reader when saving snapshot.", zap.NamedError("error", err))
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
			s.logger.Error("Failed to close snapshot file.", zap.NamedError("error", err))
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
			s.logger.Error("Prevent panic by handling failure accessing a path.",
				zap.String("snapshot", path), zap.NamedError("error", err))
			return err
		}
		if info.IsDir() {
			return nil
		}
		snap, err := types.ParseSnapshot(path)
		if err != nil {
			s.logger.Warn("Invalid snapshot found, ignoring it.", zap.String("snapshot", path))
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
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && !errors.Is(pathErr.Err, syscall.ENOTEMPTY) {
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
