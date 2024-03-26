//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package offload

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/offload"
)

// TODO adjust or make configurable
const (
	storeTimeout = 24 * time.Hour
	metaTimeout  = 20 * time.Minute

	// DefaultChunkSize if size is not specified
	DefaultChunkSize = 1 << 27 // 128MB

	// maxChunkSize is the upper bound on the chunk size
	maxChunkSize = 1 << 29 // 512MB

	// minChunkSize is the lower bound on the chunk size
	minChunkSize = 1 << 21 // 2MB

	// maxCPUPercentage max CPU percentage can be consumed by the file writer
	maxCPUPercentage = 80

	// DefaultCPUPercentage default CPU percentage can be consumed by the file writer
	DefaultCPUPercentage = 50
)

const (
	// OffloadFile used by a node to store its metadata
	OffloadFile = "offload.json"
	// GlobalBackupFile used by coordinator to store its metadata
	GlobalOffloadFile = "offload_config.json"
	GlobalOnloadFile  = "onload_config.json"
	_TempDirectory    = ".onload.tmp"
)

var _NUMCPU = runtime.NumCPU()

type objStore struct {
	b        modulecapabilities.BackupBackend
	BasePath string
}

func (s *objStore) HomeDir() string {
	return s.b.HomeDir(s.BasePath)
}

func (s *objStore) WriteToFile(ctx context.Context, key, destPath string) error {
	return s.b.WriteToFile(ctx, s.BasePath, key, destPath)
}

// SourceDataPath is data path of all source files
func (s *objStore) SourceDataPath() string {
	return s.b.SourceDataPath()
}

func (s *objStore) Write(ctx context.Context, key string, r io.ReadCloser) (int64, error) {
	return s.b.Write(ctx, s.BasePath, key, r)
}

func (s *objStore) Read(ctx context.Context, key string, w io.WriteCloser) (int64, error) {
	return s.b.Read(ctx, s.BasePath, key, w)
}

func (s *objStore) Initialize(ctx context.Context) error {
	return s.b.Initialize(ctx, s.BasePath)
}

// meta marshals and uploads metadata
func (s *objStore) putMeta(ctx context.Context, key string, desc interface{}) error {
	bytes, err := json.Marshal(desc)
	if err != nil {
		return fmt.Errorf("marshal meta file %q: %w", key, err)
	}
	ctx, cancel := context.WithTimeout(ctx, metaTimeout)
	defer cancel()
	if err := s.b.PutObject(ctx, s.BasePath, key, bytes); err != nil {
		return fmt.Errorf("upload meta file %q: %w", key, err)
	}
	return nil
}

func (s *objStore) meta(ctx context.Context, key string, dest interface{}) error {
	bytes, err := s.b.GetObject(ctx, s.BasePath, key)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, dest)
	if err != nil {
		return fmt.Errorf("marshal meta file %q: %w", key, err)
	}
	return nil
}

type nodeStore struct {
	objStore
}

// Meta gets meta data using standard path or deprecated old path
//
// adjustBasePath: sets the base path to the old path if the backup has been created prior to v1.17.
func (s *nodeStore) Meta(ctx context.Context) (*offload.OffloadNodeDescriptor, error) {
	var result offload.OffloadNodeDescriptor
	err := s.meta(ctx, OffloadFile, &result)
	// if err != nil {
	// 	cs := &objStore{s.b, backupID} // for backward compatibility
	// 	if err := cs.meta(ctx, OffloadFile, &result); err == nil {
	// 		if adjustBasePath {
	// 			s.objStore.BasePath = backupID
	// 		}
	// 		return &result, nil
	// 	}
	// }

	return &result, err
}

// meta marshals and uploads metadata
func (s *nodeStore) PutMeta(ctx context.Context, desc *offload.OffloadNodeDescriptor) error {
	return s.putMeta(ctx, OffloadFile, desc)
}

type coordStore struct {
	objStore
}

// PutMeta puts coordinator's global metadata into object store
func (s *coordStore) PutMeta(ctx context.Context, filename string, desc *offload.OffloadDistributedDescriptor) error {
	return s.putMeta(ctx, filename, desc)
}

// Meta gets coordinator's global metadata from object store
func (s *coordStore) Meta(ctx context.Context, filename string) (*offload.OffloadDistributedDescriptor, error) {
	var result offload.OffloadDistributedDescriptor
	err := s.meta(ctx, filename, &result)
	return &result, err
}

// uploader uploads backup artifacts. This includes db files and metadata
type uploader struct {
	sourcer Sourcer
	backend nodeStore
	zipConfig
	setStatus func(st offload.Status)
	log       logrus.FieldLogger
}

func newUploader(sourcer Sourcer, backend nodeStore,
	setstatus func(st offload.Status), l logrus.FieldLogger,
) *uploader {
	return &uploader{
		sourcer, backend,
		newZipConfig(Compression{
			Level:         DefaultCompression,
			CPUPercentage: DefaultCPUPercentage,
			ChunkSize:     DefaultChunkSize,
		}),
		setstatus,
		l,
	}
}

func (u *uploader) withCompression(cfg zipConfig) *uploader {
	u.zipConfig = cfg
	return u
}

// all uploads all files in addition to the metadata file
func (u *uploader) all(ctx context.Context, class string, tenant string, desc *offload.OffloadNodeDescriptor) (err error) {
	u.setStatus(offload.Transferring)
	desc.Status = string(offload.Transferring)
	shardsCh := u.sourcer.OffloadDescriptors(ctx, desc.ID, class, tenant)

	defer func() {
		//  make sure context is not cancelled when uploading metadata
		ctx := context.Background()
		if err != nil {
			desc.Error = err.Error()
			desc.Status = string(offload.Failed)
			u.setStatus(offload.Failed)
			err = fmt.Errorf("upload %w: %v", err, u.backend.PutMeta(ctx, desc))
		} else {
			u.log.Info("start uploading meta data")
			desc.Status = string(offload.Success)
			if err = u.backend.PutMeta(ctx, desc); err == nil {
				u.setStatus(offload.Success)
			}
			u.log.Info("finish uploading meta data")
		}
	}()
Loop:
	// TODO AL one for now
	for {
		select {
		case shardDesc, ok := <-shardsCh:
			if !ok {
				break Loop // we are done
			}
			if shardDesc.Error != nil {
				err = shardDesc.Error
				desc.Error = err.Error()
				desc.Node = shardDesc.Node
				return
			}

			desc.Files = shardDesc.Files
			desc.Node = shardDesc.Node

			u.log.WithField("id", desc.ID).Info("start uploading files")
			if err = u.class(ctx, desc); err != nil {
				return
			}
			u.log.WithField("id", desc.ID).Info("finish uploading files")

		case <-ctx.Done():
			err = ctx.Err()
			return
		}
	}
	u.setStatus(offload.Transferred)
	desc.Status = string(offload.Transferred)
	return nil
}

// class uploads one class
func (u *uploader) class(ctx context.Context, desc *offload.OffloadNodeDescriptor) (err error) {
	// classLabel := desc.Name
	// if monitoring.GetMetrics().Group {
	// 	classLabel = "n/a"
	// }
	// metric, err := monitoring.GetMetrics().BackupStoreDurations.GetMetricWithLabelValues(getType(u.backend.b), classLabel)
	// if err == nil {
	// 	timer := prometheus.NewTimer(metric)
	// 	defer timer.ObserveDuration()
	// }
	defer func() {
		// backups need to be released anyway
		go u.sourcer.ReleaseOffload(context.Background(), desc.ID, desc.Class, desc.Tenant)
	}()

	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()

	// TODO AL shards number (multi tenants)
	nShards := 1
	if nShards == 0 {
		return nil
	}

	// desc.Chunks = make(map[int32][]string, 1+nShards/2)
	var (
		hasJobs   atomic.Bool
		lastChunk = int32(0)
		nWorker   = u.GoPoolSize
	)
	if nWorker > nShards {
		nWorker = nShards
	}
	hasJobs.Store(nShards > 0)

	// jobs produces work for the processor
	jobs := func(xs []*offload.OffloadNodeDescriptor) <-chan *offload.OffloadNodeDescriptor {
		sendCh := make(chan *offload.OffloadNodeDescriptor)
		f := func() {
			defer close(sendCh)
			defer hasJobs.Store(false)

			for _, shard := range xs {
				select {
				case sendCh <- shard:
				// cancellation will happen for two reasons:
				//  - 1. if the whole operation has been aborted,
				//  - 2. or if the processor routine returns an error
				case <-ctx.Done():
					return
				}
			}
		}
		enterrors.GoWrapper(f, u.log)
		return sendCh
	}

	// processor
	processor := func(nWorker int, sender <-chan *offload.OffloadNodeDescriptor) <-chan chuckShards {
		eg, ctx := enterrors.NewErrorGroupWithContextWrapper(u.log, ctx)
		eg.SetLimit(nWorker)
		recvCh := make(chan chuckShards, nWorker)
		f := func() {
			defer close(recvCh)
			for i := 0; i < nWorker; i++ {
				eg.Go(func() error {
					// operation might have been aborted see comment above
					if err := ctx.Err(); err != nil {
						return err
					}
					for hasJobs.Load() {
						chunk := atomic.AddInt32(&lastChunk, 1)
						shards, err := u.compress(ctx, chunk, sender)
						if err != nil {
							return err
						}
						if m := int32(len(shards)); m > 0 {
							recvCh <- chuckShards{chunk, shards}
						}
					}
					return err
				})
			}
			err = eg.Wait()
		}
		enterrors.GoWrapper(f, u.log)
		return recvCh
	}

	for x := range processor(nWorker, jobs([]*offload.OffloadNodeDescriptor{desc})) {
		// TODO AL remove
		fmt.Printf("%v\n", x)
		// desc.Chunks[x.chunk] = x.shards
	}
	return
}

type chuckShards struct {
	chunk  int32
	shards []string
}

func (u *uploader) compress(ctx context.Context,
	chunk int32, // chunk index
	ch <-chan *offload.OffloadNodeDescriptor, // chan of shards
) ([]string, error) {
	var (
		chunkKey = chunkKey(chunk)
		shards   = make([]string, 0, 10)
		// add tolerance to enable better optimization of the chunk size
		maxSize = int64(u.ChunkSize + u.ChunkSize/20) // size + 5%
	)
	zip, reader := NewZip(u.backend.SourceDataPath(), u.Level)
	producer := func() error {
		defer zip.Close()
		lastShardSize := int64(0)
		for shard := range ch {
			if _, err := zip.WriteShard(ctx, shard); err != nil {
				return err
			}
			// shard.Chunk = chunk
			// shards = append(shards, shard.Name)
			// shard.ClearTemporary()

			zip.gzw.Flush() // flush new shard
			lastShardSize = zip.lastWritten() - lastShardSize
			if zip.lastWritten()+lastShardSize > maxSize {
				break
			}
		}
		return nil
	}

	// consumer
	eg := enterrors.NewErrorGroupWrapper(u.log)
	eg.Go(func() error {
		if _, err := u.backend.Write(ctx, chunkKey, reader); err != nil {
			return err
		}
		return nil
	})

	if err := producer(); err != nil {
		return shards, err
	}
	// wait for the consumer to finish
	return shards, eg.Wait()
}

// fileWriter downloads files from object store and writes files to the destination folder destDir
type fileWriter struct {
	sourcer    Sourcer
	backend    nodeStore
	tempDir    string
	destDir    string
	movedFiles []string // files successfully moved to destination folder
	GoPoolSize int
	logger     logrus.FieldLogger
}

func newFileWriter(sourcer Sourcer, backend nodeStore,
	logger logrus.FieldLogger,
) *fileWriter {
	destDir := backend.SourceDataPath()
	return &fileWriter{
		sourcer:    sourcer,
		backend:    backend,
		destDir:    destDir,
		tempDir:    path.Join(destDir, _TempDirectory),
		movedFiles: make([]string, 0, 64),
		GoPoolSize: routinePoolSize(50),
		logger:     logger,
	}
}

func (fw *fileWriter) WithPoolPercentage(p int) *fileWriter {
	fw.GoPoolSize = routinePoolSize(p)
	return fw
}

// Write downloads files and put them in the destination directory
func (fw *fileWriter) Write(ctx context.Context, desc *offload.OffloadNodeDescriptor) (rollback func() error, err error) {
	// if len(desc.Shards) == 0 { // nothing to copy
	// 	return func() error { return nil }, nil
	// }
	shardTempDir := path.Join(fw.tempDir, desc.ID)
	defer func() {
		if err != nil {
			if rerr := fw.rollBack(); rerr != nil {
				err = fmt.Errorf("%w: %v", err, rerr)
			}
		}
		os.RemoveAll(shardTempDir)
	}()

	if err := fw.writeTempFiles(ctx, shardTempDir, desc); err != nil {
		return nil, fmt.Errorf("get files: %w", err)
	}
	if err := fw.moveAll(shardTempDir); err != nil {
		return nil, fmt.Errorf("move files to destination: %w", err)
	}
	return fw.rollBack, nil
}

// writeTempFiles writes class files into a temporary directory
// temporary directory path = d.tempDir/className
// Function makes sure that created files will be removed in case of an error
func (fw *fileWriter) writeTempFiles(ctx context.Context, shardTempDir string, desc *offload.OffloadNodeDescriptor) (err error) {
	if err := os.RemoveAll(shardTempDir); err != nil {
		return fmt.Errorf("remove %s: %w", shardTempDir, err)
	}
	if err := os.MkdirAll(shardTempDir, os.ModePerm); err != nil {
		return fmt.Errorf("create temp class folder %s: %w", shardTempDir, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(fw.logger, ctx)
	eg.SetLimit(fw.GoPoolSize)
	// for k := range desc.Chunks {
	chunk := chunkKey(1)
	eg.Go(func() error {
		uz, w := NewUnzip(shardTempDir)
		go func() {
			fw.backend.Read(ctx, chunk, w)
		}()
		_, err := uz.ReadChunk()
		return err
	})
	// }
	return eg.Wait()
}

func (fw *fileWriter) writeTempShard(ctx context.Context, sd *backup.ShardDescriptor, classTempDir string) error {
	for _, key := range sd.Files {
		destPath := path.Join(classTempDir, key)
		destDir := path.Dir(destPath)
		if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
			return fmt.Errorf("create folder %s: %w", destDir, err)
		}
		if err := fw.backend.WriteToFile(ctx, key, destPath); err != nil {
			return fmt.Errorf("write file %s: %w", destPath, err)
		}
	}
	destPath := path.Join(classTempDir, sd.DocIDCounterPath)
	if err := os.WriteFile(destPath, sd.DocIDCounter, os.ModePerm); err != nil {
		return fmt.Errorf("write counter file %s: %w", destPath, err)
	}
	destPath = path.Join(classTempDir, sd.PropLengthTrackerPath)
	if err := os.WriteFile(destPath, sd.PropLengthTracker, os.ModePerm); err != nil {
		return fmt.Errorf("write prop file %s: %w", destPath, err)
	}
	destPath = path.Join(classTempDir, sd.ShardVersionPath)
	if err := os.WriteFile(destPath, sd.Version, os.ModePerm); err != nil {
		return fmt.Errorf("write version file %s: %w", destPath, err)
	}
	return nil
}

// moveAll moves all files to the destination
func (fw *fileWriter) moveAll(classTempDir string) (err error) {
	files, err := os.ReadDir(classTempDir)
	if err != nil {
		return fmt.Errorf("read %s", classTempDir)
	}
	destDir := fw.destDir
	for _, key := range files {
		from := path.Join(classTempDir, key.Name())
		to := path.Join(destDir, key.Name())
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("move %s %s: %w", from, to, err)
		}
		fw.movedFiles = append(fw.movedFiles, to)
	}

	return nil
}

// rollBack successfully written files
func (fw *fileWriter) rollBack() (err error) {
	// rollback successfully moved files
	for _, fpath := range fw.movedFiles {
		if rerr := os.RemoveAll(fpath); rerr != nil && err == nil {
			err = fmt.Errorf("rollback %s: %w", fpath, rerr)
		}
	}
	return err
}

func chunkKey(id int32) string {
	return fmt.Sprintf("chunk-%d", id)
}

func routinePoolSize(percentage int) int {
	if percentage == 0 { // default value
		percentage = DefaultCPUPercentage
	} else if percentage > maxCPUPercentage {
		percentage = maxCPUPercentage
	}
	if x := (_NUMCPU * percentage) / 100; x > 0 {
		return x
	}
	return 1
}
