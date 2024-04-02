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
	"path/filepath"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/offload"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func (s *Shard) offloadDescriptor(ctx context.Context, desc *offload.ShardDescriptor) (err error) {
	desc.Node = s.nodeName()

	onError := func(callback func()) {
		if err != nil {
			callback()
		}
	}

	// set shard to readonly
	ready := storagestate.StatusReady.String()
	readonly := storagestate.StatusReadOnly.String()
	if _, err := s.compareAndSwapStatus(ready, readonly); err != nil {
		return err
	}
	defer onError(func() { s.compareAndSwapStatus(readonly, ready) })

	// prevent parallel backup
	s.index.backupMutex.RLock()
	defer onError(func() { s.index.backupMutex.RUnlock() })

	// mark ongoing offload
	// TODO AL offloadId
	if err := s.initOngoingOffload("some id"); err != nil {
		return err
	}
	defer onError(func() { s.resetOngoingOffload() })

	// pause maintenance cycles
	if err = s.pauseMaintenanceCyclesForOffload(ctx); err != nil {
		return fmt.Errorf("pause maintenance cycles: %w", err)
	}
	defer onError(func() {
		go func() {
			if err := s.resumeMaintenanceCycles(ctx); err != nil {
				s.index.logger.
					WithField("shard", s.name).
					WithField("op", "resume_maintenance").
					Error(err)
			}
		}()
	})

	desc.Files, err = s.listOffloadFiles(ctx)
	if err != nil {
		return fmt.Errorf("list shard %q files: %w", s.name, err)
	}

	return nil
}

// pauseMaintenanceCyclesForOffload stops compaction, flushes memtable and switches commit logs
// to begin with the shard's offload
func (s *Shard) pauseMaintenanceCyclesForOffload(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
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

func (s *Shard) listOffloadFiles(ctx context.Context) ([]string, error) {
	rootPath := s.index.Config.RootPath
	files := make([]string, 0, 32)

	// meta files
	for metaName, metaFile := range map[string]string{
		"docid counter":      s.counter.FileName(),
		"proplength tracker": s.GetPropertyLengthTracker().FileName(),
		"shard version":      s.versioner.path,
	} {
		file, err := filepath.Rel(rootPath, metaFile)
		if err != nil {
			return nil, fmt.Errorf("%s path: %w", metaName, err)
		}
		files = append(files, file)
	}

	// store files
	storeFiles, err := s.store.ListFiles(ctx, rootPath)
	if err != nil {
		return nil, err
	}
	files = append(files, storeFiles...)

	// vector index files
	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			vectorFiles, err := vectorIndex.ListFiles(ctx, rootPath)
			if err != nil {
				return nil, fmt.Errorf("list files of vector %q: %w", targetVector, err)
			}
			files = append(files, vectorFiles...)
		}
	} else {
		vectorFiles, err := s.vectorIndex.ListFiles(ctx, rootPath)
		if err != nil {
			return nil, err
		}
		files = append(files, vectorFiles...)
	}

	return files, nil
}

func (s *Shard) releaseSuccessfulOffload(ctx context.Context) error {
	// do not resume maintenance cycles
	// do not reset ongoing offload
	s.index.backupMutex.RUnlock()
	// do not change status

	return nil
}

func (s *Shard) releaseFailedOffload(ctx context.Context) error {
	err1 := s.resumeMaintenanceCycles(ctx)
	s.resetOngoingOffload()
	s.index.backupMutex.RUnlock()
	_, err2 := s.compareAndSwapStatus(storagestate.StatusReadOnly.String(), storagestate.StatusReady.String())

	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(err1)
	ec.Add(err2)
	return ec.ToError()
}

// TODO AL is status necessary?
func (s *Shard) initOngoingOffload(offloadId string) error {
	if !s.ongoingOffload.CompareAndSwap(nil, &offloadId) {
		return fmt.Errorf(
			"cannot offload tenant, previous offload is not yet finished, " +
				"this means its contents have not yet been fully copied to its destination, " +
				"try again later")
	}
	return nil
}

// TODO AL is status necessary?
func (s *Shard) resetOngoingOffload() {
	s.ongoingOffload.Store(nil)
}
