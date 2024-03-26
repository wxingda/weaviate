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

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/offload"
	"github.com/weaviate/weaviate/entities/storagestate"

	"github.com/weaviate/weaviate/entities/backup"
)

// BeginBackup stops compaction, and flushing memtable and commit log to begin with the backup
func (s *Shard) BeginBackup(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.resumeMaintenanceCycles(ctx); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %v", err, err2)
			}
		}
	}()
	if err = s.store.PauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction: %w", err)
	}
	if err = s.store.FlushMemtables(ctx); err != nil {
		return fmt.Errorf("flush memtables: %w", err)
	}
	if err = s.cycleCallbacks.vectorCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause vector maintenance: %w", err)
	}
	if err = s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause geo props maintenance: %w", err)
	}
	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			if err = vectorIndex.SwitchCommitLogs(ctx); err != nil {
				return fmt.Errorf("switch commit logs of vector %q: %w", targetVector, err)
			}
		}
	} else {
		if err = s.vectorIndex.SwitchCommitLogs(ctx); err != nil {
			return fmt.Errorf("switch commit logs: %w", err)
		}
	}
	return nil
}

// BeginBackup stops compaction, and flushing memtable and commit log to begin with the backup
func (s *Shard) BeginOffload(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.resumeMaintenanceCycles(ctx); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %v", err, err2)
			}
		}
	}()
	if err = s.store.PauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction: %w", err)
	}
	if err = s.store.FlushMemtablesIgnoreReadonly(ctx); err != nil {
		return fmt.Errorf("flush memtables: %w", err)
	}
	if err = s.cycleCallbacks.vectorCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause vector maintenance: %w", err)
	}
	if err = s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause geo props maintenance: %w", err)
	}
	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			if err = vectorIndex.SwitchCommitLogs(ctx); err != nil {
				return fmt.Errorf("switch commit logs of vector %q: %w", targetVector, err)
			}
		}
	} else {
		if err = s.vectorIndex.SwitchCommitLogs(ctx); err != nil {
			return fmt.Errorf("switch commit logs: %w", err)
		}
	}
	return nil
}

// ListBackupFiles lists all files used to backup a shard
func (s *Shard) ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error {
	var err error
	if err := s.readBackupMetadata(ret); err != nil {
		return err
	}

	if ret.Files, err = s.store.ListFiles(ctx, s.index.Config.RootPath); err != nil {
		return err
	}

	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			files, err := vectorIndex.ListFiles(ctx, s.index.Config.RootPath)
			if err != nil {
				return fmt.Errorf("list files of vector %q: %w", targetVector, err)
			}
			ret.Files = append(ret.Files, files...)
		}
	} else {
		files, err := s.vectorIndex.ListFiles(ctx, s.index.Config.RootPath)
		if err != nil {
			return err
		}
		ret.Files = append(ret.Files, files...)
	}

	return nil
}

func (s *Shard) ListOffloadFiles(ctx context.Context, desc *offload.ShardDescriptor) error {
	files := make([]string, 0, 32)

	for errFileType, filename := range map[string]string{
		"docid counter path":      s.counter.FileName(),
		"proplength tracker path": s.GetPropertyLengthTracker().FileName(),
		"shard version path":      s.versioner.path,
	} {
		file, err := filepath.Rel(s.index.Config.RootPath, filename)
		if err != nil {
			return fmt.Errorf("%s: %w", errFileType, err)
		}
		files = append(files, file)
	}

	storeFiles, err := s.store.ListFiles(ctx, s.index.Config.RootPath)
	if err != nil {
		return err
	}
	files = append(files, storeFiles...)

	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			vectorFiles, err := vectorIndex.ListFiles(ctx, s.index.Config.RootPath)
			if err != nil {
				return fmt.Errorf("list files of vector %q: %w", targetVector, err)
			}
			files = append(files, vectorFiles...)
		}
	} else {
		vectorFiles, err := s.vectorIndex.ListFiles(ctx, s.index.Config.RootPath)
		if err != nil {
			return err
		}
		files = append(files, vectorFiles...)
	}

	desc.Files = files
	return nil
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	g := enterrors.NewErrorGroupWrapper(s.index.logger)

	g.Go(func() error {
		return s.store.ResumeCompaction(ctx)
	})
	g.Go(func() error {
		return s.cycleCallbacks.vectorCombinedCallbacksCtrl.Activate()
	})
	g.Go(func() error {
		return s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Activate()
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to resume maintenance cycles for shard '%s': %w", s.name, err)
	}

	return nil
}

func (s *Shard) readBackupMetadata(d *backup.ShardDescriptor) (err error) {
	d.Name = s.name
	d.Node = s.nodeName()
	fpath := s.counter.FileName()
	if d.DocIDCounter, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard doc-id-counter %s: %w", fpath, err)
	}
	d.DocIDCounterPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("docid counter path: %w", err)
	}
	fpath = s.GetPropertyLengthTracker().FileName()
	if d.PropLengthTracker, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard prop-lengths %s: %w", fpath, err)
	}
	d.PropLengthTrackerPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("proplength tracker path: %w", err)
	}
	fpath = s.versioner.path
	if d.Version, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard version %s: %w", fpath, err)
	}
	d.ShardVersionPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("shard version path: %w", err)
	}
	return nil
}

func (s *Shard) nodeName() string {
	node, _ := s.index.getSchema.ShardOwner(
		s.index.Config.ClassName.String(), s.name)
	return node
}

// TODO AL is status necessary?
func (s *Shard) initOngoingOffload(offloadId string) error {
	if !s.ongoingOffload.CompareAndSwap(nil, &offloadId) {
		return fmt.Errorf(
			"cannot offload tenant, offload ‘%s’ is not yet finished, this "+
				"means its contents have not yet been fully copied to its destination, "+
				"try again later", offloadId)
	}
	return nil
}

// TODO AL is status necessary?
func (s *Shard) resetOngoingOffload() {
	s.ongoingOffload.Store(nil)
}

func (s *Shard) offloadDescriptor(ctx context.Context, offloadId string, desc *offload.ShardDescriptor) (err error) {
	desc.Node = s.nodeName()

	// set shard to readolny
	if _, err := s.compareAndSwapStatus(storagestate.StatusReady.String(), storagestate.StatusReadOnly.String()); err != nil {
		return err
	}

	// prevent parallel backup
	s.index.backupMutex.RLock()
	if err := s.initOngoingOffload(offloadId); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			go func() {
				if err := s.resumeMaintenanceCycles(ctx); err != nil {
					s.index.logger.
						WithField("shard", s.name).
						WithField("op", "resume_maintenance").
						Error(err)
				}
				s.resetOngoingOffload()
				s.index.backupMutex.RUnlock()
				s.compareAndSwapStatus(storagestate.StatusReadOnly.String(), storagestate.StatusReady.String())
			}()
		}
	}()

	if err = s.BeginOffload(ctx); err != nil {
		return fmt.Errorf("pause compaction and flush: %w", err)
	}
	if err = s.ListOffloadFiles(ctx, desc); err != nil {
		return fmt.Errorf("list shard %v files: %w", s.name, err)
	}

	return nil
}

func (s *Shard) releaseOffload(ctx context.Context, id, class, tenant string) error {
	// do not resume maintenance cycles
	// do not reset ongoing offload
	s.index.backupMutex.RUnlock()
	// do not change status
	return nil
}
