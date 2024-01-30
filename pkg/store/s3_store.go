package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/miaojuncn/etcd-ops/pkg/types"
	"github.com/miaojuncn/etcd-ops/pkg/zlog"
)

const (
	s3NoOfChunk       int64 = 9999
	awsCredentialFile       = "AWS_APPLICATION_CREDENTIALS"
)

type awsCredentials struct {
	AccessKeyID        string  `json:"accessKeyID"`
	Region             string  `json:"region"`
	SecretAccessKey    string  `json:"secretAccessKey"`
	BucketName         string  `json:"bucketName"`
	Endpoint           *string `json:"endpoint,omitempty"`
	S3ForcePathStyle   *bool   `json:"s3ForcePathStyle,omitempty"`
	InsecureSkipVerify *bool   `json:"insecureSkipVerify,omitempty"`
	TrustedCaCert      *string `json:"trustedCaCert,omitempty"`
}

// S3Store is snap store with AWS S3 object store as backend
type S3Store struct {
	prefix                  string
	client                  s3iface.S3API
	bucket                  string
	multiPart               sync.Mutex
	maxParallelChunkUploads uint
	minChunkSize            int64
}

// NewS3Store create new S3Store from shared configuration with specified bucket
func NewS3Store(config *types.StoreConfig) (*S3Store, error) {
	ao, err := getSessionOptions()
	if err != nil {
		return nil, err
	}
	return newS3FromSessionOpt(config.Bucket, config.Prefix, config.MaxParallelChunkUploads, config.MinChunkSize, ao)
}

// newS3FromSessionOpt will create the new S3 store object from S3 session options
func newS3FromSessionOpt(bucket, prefix string, maxParallelChunkUploads uint, minChunkSize int64, so session.Options) (*S3Store, error) {
	sess, err := session.NewSessionWithOptions(so)
	if err != nil {
		return nil, fmt.Errorf("new AWS session failed: %v", err)
	}
	cli := s3.New(sess)

	// tmpFile should create prefix directory first
	if len(prefix) != 0 {
		err := os.MkdirAll(prefix, 0700)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}

	return NewS3FromClient(bucket, prefix, maxParallelChunkUploads, minChunkSize, cli), nil
}

func getSessionOptions() (session.Options, error) {
	if dir, isSet := os.LookupEnv(awsCredentialFile); isSet {
		ao, err := readAWSCredentialFiles(dir)
		if err != nil {
			return session.Options{}, fmt.Errorf("error getting credentials from %v directory", dir)
		}
		return ao, nil
	}
	return session.Options{
		// Setting this is equal to the AWS_SDK_LOAD_CONFIG environment variable was set.
		// We want to save the work to set AWS_SDK_LOAD_CONFIG=1 outside.
		SharedConfigState: session.SharedConfigEnable,
	}, nil
}

func readAWSCredentialFiles(dirname string) (session.Options, error) {
	awsConfig, err := readAWSCredentialFromDir(dirname)
	if err != nil {
		return session.Options{}, err
	}

	httpClient := http.DefaultClient
	if awsConfig.InsecureSkipVerify != nil && *awsConfig.InsecureSkipVerify == true {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: *awsConfig.InsecureSkipVerify},
		}
	}

	if awsConfig.TrustedCaCert != nil {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(*awsConfig.TrustedCaCert))
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:            caCertPool,
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS13,
			},
		}
	}

	return session.Options{
		Config: aws.Config{
			Credentials:      credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, ""),
			Region:           &awsConfig.Region,
			Endpoint:         awsConfig.Endpoint,
			S3ForcePathStyle: awsConfig.S3ForcePathStyle,
			HTTPClient:       httpClient,
		},
	}, nil
}

func readAWSCredentialFromDir(dirname string) (*awsCredentials, error) {
	awsConfig := &awsCredentials{}

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		switch file.Name() {
		case "accessKeyID":
			data, err := os.ReadFile(dirname + "/accessKeyID")
			if err != nil {
				return nil, err
			}
			awsConfig.AccessKeyID = string(data)
		case "region":
			data, err := os.ReadFile(dirname + "/region")
			if err != nil {
				return nil, err
			}
			awsConfig.Region = string(data)
		case "secretAccessKey":
			data, err := os.ReadFile(dirname + "/secretAccessKey")
			if err != nil {
				return nil, err
			}
			awsConfig.SecretAccessKey = string(data)
		case "endpoint":
			data, err := os.ReadFile(dirname + "/endpoint")
			if err != nil {
				return nil, err
			}
			tmp := string(data)
			awsConfig.Endpoint = &tmp
		case "s3ForcePathStyle":
			data, err := os.ReadFile(dirname + "/s3ForcePathStyle")
			if err != nil {
				return nil, err
			}
			val, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			awsConfig.S3ForcePathStyle = &val
		case "insecureSkipVerify":
			data, err := os.ReadFile(dirname + "/insecureSkipVerify")
			if err != nil {
				return nil, err
			}
			val, err := strconv.ParseBool(string(data))
			if err != nil {
				return nil, err
			}
			awsConfig.InsecureSkipVerify = &val
		case "trustedCaCert":
			data, err := os.ReadFile(dirname + "/trustedCaCert")
			if err != nil {
				return nil, err
			}
			tmp := string(data)
			awsConfig.TrustedCaCert = &tmp
		}
	}

	if err := isAWSConfigEmpty(awsConfig); err != nil {
		return nil, err
	}
	return awsConfig, nil
}

// NewS3FromClient will create the new S3 store object from S3 client
func NewS3FromClient(bucket, prefix string, maxParallelChunkUploads uint, minChunkSize int64, cli s3iface.S3API) *S3Store {
	return &S3Store{
		bucket:                  bucket,
		prefix:                  prefix,
		client:                  cli,
		maxParallelChunkUploads: maxParallelChunkUploads,
		minChunkSize:            minChunkSize,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *S3Store) Fetch(snap types.Snapshot) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return nil, fmt.Errorf("error while accessing %s: %v", path.Join(s.prefix, snap.SnapDir, snap.SnapName), err)
	}
	return resp.Body, nil
}

// Save will write the snapshot to store
func (s *S3Store) Save(snap types.Snapshot, rc io.ReadCloser) error {
	defer func() {
		if err := rc.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close reader when saving snapshot: %v", err)
		}
	}()
	tmpFile, err := os.CreateTemp(s.prefix, TmpBackupFilePrefix)
	if err != nil {
		return fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		if err := tmpFile.Close(); err != nil {
			zlog.Logger.Errorf("Failed to close temp file when saving snapshot: %v", err)
		}

		if err := os.Remove(tmpFile.Name()); err != nil {
			zlog.Logger.Errorf("Failed to remove temp file when saving snapshot: %v", err)
		}
	}()

	size, err := io.Copy(tmpFile, rc)
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpFile: %v", err)
	}
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	// Initiate multi part upload
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
	defer cancel()
	uploadOutput, err := s.client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload %v", err)
	}
	zlog.Logger.Infof("Successfully initiated the multipart upload with upload ID : %s", *uploadOutput.UploadId)

	var (
		chunkSize  = int64(math.Max(float64(s.minChunkSize), float64(size/s3NoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}
	var (
		completedParts = make([]*s3.CompletedPart, noOfChunks)
		chunkUploadCh  = make(chan chunk, noOfChunks)
		resCh          = make(chan chunkUploadResult, noOfChunks)
		wg             sync.WaitGroup
		cancelCh       = make(chan struct{})
	)

	for i := uint(0); i < s.maxParallelChunkUploads; i++ {
		wg.Add(1)
		go s.partUploader(&wg, cancelCh, &snap, tmpFile, uploadOutput.UploadId, completedParts, chunkUploadCh, resCh)
	}
	zlog.Logger.Infof("Uploading snapshot of size: %d, chunkSize: %d, noOfChunks: %d", size, chunkSize, noOfChunks)

	for offset, index := int64(0), 1; offset < size; offset += chunkSize {
		newChunk := chunk{
			id:     index,
			offset: offset,
			size:   chunkSize,
		}
		zlog.Logger.Debugf("Triggering chunk upload for offset: %d", offset)
		chunkUploadCh <- newChunk
		index++
	}
	zlog.Logger.Infof("Triggered chunk upload for all chunks, total: %d", noOfChunks)
	snapshotErr := collectChunkUploadError(chunkUploadCh, resCh, cancelCh, noOfChunks)
	wg.Wait()

	if snapshotErr != nil {
		ctx := context.TODO()
		ctx, cancel := context.WithTimeout(ctx, chunkUploadTimeout)
		defer cancel()
		zlog.Logger.Infof("Aborting the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
		})
	} else {
		ctx = context.TODO()
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		zlog.Logger.Infof("Finishing the multipart upload with upload ID : %s", *uploadOutput.UploadId)
		_, err = s.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &s.bucket,
			Key:      aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
			UploadId: uploadOutput.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
	}

	if err != nil {
		return fmt.Errorf("failed completing snapshot upload with error %v", err)
	}
	if snapshotErr != nil {
		return fmt.Errorf("failed uploading chunk, id: %d, offset: %d, error: %v", snapshotErr.chunk.id, snapshotErr.chunk.offset, snapshotErr.err)
	}
	return nil
}

func (s *S3Store) uploadPart(snap *types.Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, offset, chunkSize int64) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size() - offset
	if size > chunkSize {
		size = chunkSize
	}

	sr := io.NewSectionReader(file, offset, size)
	ctx, cancel := context.WithTimeout(context.TODO(), chunkUploadTimeout)
	defer cancel()
	partNumber := (offset / chunkSize) + 1
	in := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
		PartNumber: &partNumber,
		UploadId:   uploadID,
		Body:       sr,
	}

	part, err := s.client.UploadPartWithContext(ctx, in)
	if err == nil {
		completedPart := &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: &partNumber,
		}
		completedParts[partNumber-1] = completedPart
	}
	return err
}

func (s *S3Store) partUploader(wg *sync.WaitGroup, stopCh <-chan struct{}, snap *types.Snapshot, file *os.File, uploadID *string, completedParts []*s3.CompletedPart, chunkUploadCh <-chan chunk, errCh chan<- chunkUploadResult) {
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		case chunk, ok := <-chunkUploadCh:
			if !ok {
				return
			}
			zlog.Logger.Infof("Uploading chunk with id: %d, offset: %d, attempt: %d", chunk.id, chunk.offset, chunk.attempt)
			err := s.uploadPart(snap, file, uploadID, completedParts, chunk.offset, chunk.size)
			errCh <- chunkUploadResult{
				err:   err,
				chunk: &chunk,
			}
		}
	}
}

// List will return sorted list with all snapshot files on store.
func (s *S3Store) List() (types.SnapList, error) {
	var snapList types.SnapList
	in := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.prefix),
	}
	err := s.client.ListObjectsPages(in, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range page.Contents {
			k := (*object.Key)[len(*page.Prefix):]
			snap, err := types.ParseSnapshot(path.Join(s.prefix, k))
			if err != nil {
				zlog.Logger.Warnf("Invalid snapshot found. Ignoring it: %s", k)
			} else {
				snapList = append(snapList, snap)
			}

		}
		return !lastPage
	})
	if err != nil {
		return nil, err
	}

	sort.Sort(snapList)
	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *S3Store) Delete(snap types.Snapshot) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path.Join(s.prefix, snap.SnapDir, snap.SnapName)),
	})
	return err
}

// S3StoreHash calculates and returns the hash of aws S3 store secret.
func S3StoreHash() (string, error) {
	if dir, isSet := os.LookupEnv(awsCredentialFile); isSet {
		awsConfig, err := readAWSCredentialFromDir(dir)
		if err != nil {
			return "", fmt.Errorf("error getting credentials from %v directory", dir)
		}
		return getS3Hash(awsConfig), nil
	}
	return "", nil
}

func getS3Hash(config *awsCredentials) string {
	data := fmt.Sprintf("%s%s%s", config.AccessKeyID, config.SecretAccessKey, config.Region)
	return getHash(data)
}

func isAWSConfigEmpty(config *awsCredentials) error {
	if len(config.AccessKeyID) != 0 && len(config.Region) != 0 && len(config.SecretAccessKey) != 0 {
		return nil
	}
	return fmt.Errorf("aws s3 credentials: region, secretAccessKey or accessKeyID is missing")
}
