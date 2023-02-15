package store

import (
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"go.uber.org/zap"
)

// OSSBucket is an interface for oss.Bucket used in snapstore
type OSSBucket interface {
	GetObject(objectKey string, options ...oss.Option) (io.ReadCloser, error)
	InitiateMultipartUpload(objectKey string, options ...oss.Option) (oss.InitiateMultipartUploadResult, error)
	CompleteMultipartUpload(imur oss.InitiateMultipartUploadResult, parts []oss.UploadPart, options ...oss.Option) (oss.CompleteMultipartUploadResult, error)
	ListObjects(options ...oss.Option) (oss.ListObjectsResult, error)
	DeleteObject(objectKey string, options ...oss.Option) error
	UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, options ...oss.Option) (oss.UploadPart, error)
	AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, options ...oss.Option) error
}

const (
	// Total number of chunks to be uploaded must be one less than maximum limit allowed.
	ossNoOfChunk int64 = 9999
	// The secret file should be provided and file path should be made available as environment variables: `ALICLOUD_APPLICATION_CREDENTIALS`.
	aliCredentialFile = "ALICLOUD_APPLICATION_CREDENTIALS"
)

type authOptions struct {
	Endpoint   string `json:"storageEndpoint"`
	AccessID   string `json:"accessKeyID"`
	AccessKey  string `json:"accessKeySecret"`
	BucketName string `json:"bucketName"`
}

// OSSStore is snapstore with Alicloud OSS object store as backend
type OSSStore struct {
	prefix                  string
	bucket                  OSSBucket
	multiPart               sync.Mutex
	maxParallelChunkUploads uint
	minChunkSize            int64
}

// NewOSSStore create new OSS Store from shared configuration with specified bucket
func NewOSSStore(config *types.StoreConfig) (*OSSStore, error) {
	ao, err := getAuthOptions()
	if err != nil {
		return nil, err
	}
	return newOSSFromAuthOpt(config.Bucket, config.Prefix, config.MaxParallelChunkUploads, config.MinChunkSize, *ao)
}

func newOSSFromAuthOpt(bucket, prefix string, maxParallelChunkUploads uint, minChunkSize int64, ao authOptions) (*OSSStore, error) {
	client, err := oss.New(ao.Endpoint, ao.AccessID, ao.AccessKey)
	if err != nil {
		return nil, err
	}

	bucketOSS, err := client.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	// tmpFile should create prefix directory first
	if len(prefix) != 0 {
		err := os.MkdirAll(prefix, 0700)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}

	return NewOSSFromBucket(prefix, maxParallelChunkUploads, minChunkSize, bucketOSS), nil
}

// NewOSSFromBucket will create the new OSS store object from OSS bucket
func NewOSSFromBucket(prefix string, maxParallelChunkUploads uint, minChunkSize int64, bucket OSSBucket) *OSSStore {
	return &OSSStore{
		prefix:                  prefix,
		bucket:                  bucket,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *OSSStore) Fetch(snap types.Snapshot) (io.ReadCloser, error) {
	body, err := s.bucket.GetObject(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Save will write the snapshot to store
func (s *OSSStore) Save(snap types.Snapshot, rc io.ReadCloser) error {
	tmpFile, err := os.CreateTemp(s.prefix, TmpBackupFilePrefix)
	if err != nil {
		rc.Close()
		return fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	size, err := io.Copy(tmpFile, rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpFile: %v", err)
	}
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var (
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/ossNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	ossChunks, err := oss.SplitFileByPartNum(tmpFile.Name(), int(noOfChunks))
	if err != nil {
		return err
	}

	imur, err := s.bucket.InitiateMultipartUpload(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}
	var (
		completedParts = make([]oss.UploadPart, noOfChunks)
		chunkUploadCh  = make(chan chunk, noOfChunks)
		resCh          = make(chan chunkUploadResult, noOfChunks)
		cancelCh       = make(chan struct{})
		wg             sync.WaitGroup
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.partUploader(&wg, imur, tmpFile, completedParts, chunkUploadCh, cancelCh, resCh)
	}

	for _, ossChunk := range ossChunks {
		chunk := chunk{
			offset: ossChunk.Offset,
			size:   ossChunk.Size,
			id:     ossChunk.Number,
		}
		zap.S().Debugf("Triggering chunk upload for offset: %d", chunk.offset)
		chunkUploadCh <- chunk
	}

	zap.S().Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)
	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr == nil {
		_, err := s.bucket.CompleteMultipartUpload(imur, completedParts)
		if err != nil {
			return err
		}
		zap.S().Infof("Finishing the multipart upload with upload ID : %s", imur.UploadID)
	} else {
		zap.S().Infof("Aborting the multipart upload with upload ID : %s", imur.UploadID)
		err := s.bucket.AbortMultipartUpload(imur)
		if err != nil {
			return snapshotErr.err
		}
	}

	return nil
}

func (s *OSSStore) partUploader(wg *sync.WaitGroup, imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, chunkUploadCh <-chan chunk, stopCh <-chan struct{}, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			zap.S().Infof("Uploading chunk with id: %d, offset: %d, size: %d", chunk.id, chunk.offset, chunk.size)
			err := s.uploadPart(imur, file, completedParts, chunk.offset, chunk.size, chunk.id)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

func (s *OSSStore) uploadPart(imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, offset, chunkSize int64, number int) error {
	fd := io.NewSectionReader(file, offset, chunkSize)
	part, err := s.bucket.UploadPart(imur, fd, chunkSize, number)

	if err == nil {
		completedParts[number-1] = part
	}
	return err
}

// List will return sorted list with all snapshot files on store.
func (s *OSSStore) List() (types.SnapList, error) {
	// prefixTokens := strings.Split(s.prefix, "/")
	// prefix := path.Join(strings.Join(prefixTokens[:len(prefixTokens)-1], "/"))

	var snapList types.SnapList

	marker := ""
	for {
		lsRes, err := s.bucket.ListObjects(oss.Marker(marker), oss.Prefix(s.prefix))
		if err != nil {
			return nil, err
		}
		for _, object := range lsRes.Objects {
			snap, err := types.ParseSnapshot(object.Key)
			if err != nil {
				// Warning
				zap.S().Warnf("Invalid snapshot found. Ignoring it: %s", object.Key)
			} else {
				snapList = append(snapList, snap)
			}
		}
		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}
	sort.Sort(snapList)

	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *OSSStore) Delete(snap types.Snapshot) error {
	return s.bucket.DeleteObject(path.Join(s.prefix, snap.SnapDir, snap.SnapName))
}

func getAuthOptions() (*authOptions, error) {

	if dir, isSet := os.LookupEnv(aliCredentialFile); isSet {
		ao, err := readALICredentialFiles(dir)
		if err != nil {
			return nil, fmt.Errorf("error getting credentials from %v directory", dir)
		}
		return ao, nil
	}

	return nil, fmt.Errorf("unable to get oss credentials")
}

func readALICredentialFiles(dirname string) (*authOptions, error) {
	aliConfig := &authOptions{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == "storageEndpoint" {
			data, err := os.ReadFile(dirname + "/storageEndpoint")
			if err != nil {
				return nil, err
			}
			aliConfig.Endpoint = strings.Trim(string(data), "\n")
		} else if file.Name() == "accessKeySecret" {
			data, err := os.ReadFile(dirname + "/accessKeySecret")
			if err != nil {
				return nil, err
			}
			aliConfig.AccessKey = strings.Trim(string(data), "\n")
		} else if file.Name() == "accessKeyID" {
			data, err := os.ReadFile(dirname + "/accessKeyID")
			if err != nil {
				return nil, err
			}
			aliConfig.AccessID = strings.Trim(string(data), "\n")
		}
	}

	if err := isOSSConfigEmpty(aliConfig); err != nil {
		return nil, err
	}
	return aliConfig, nil
}

// OSSStoreHash calculates and returns the hash of aliCloud OSS snapstore secret.
func OSSStoreHash(config *types.StoreConfig) (string, error) {
	if _, isSet := os.LookupEnv(aliCredentialFile); isSet {
		if dir := os.Getenv(aliCredentialFile); dir != "" {
			aliConfig, err := readALICredentialFiles(dir)
			if err != nil {
				return "", fmt.Errorf("error getting credentials from %v directory", dir)
			}
			return getOSSHash(aliConfig), nil
		}
	}

	return "", nil
}

func getOSSHash(config *authOptions) string {
	data := fmt.Sprintf("%s%s%s", config.AccessID, config.AccessKey, config.Endpoint)
	return getHash(data)
}

func isOSSConfigEmpty(config *authOptions) error {
	if len(config.AccessID) != 0 && len(config.AccessKey) != 0 && len(config.Endpoint) != 0 {
		return nil
	}
	return fmt.Errorf("aliCloud OSS credentials: accessKeyID, accessKeySecret or storageEndpoint is missing")
}
