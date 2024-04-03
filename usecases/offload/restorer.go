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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/offload"
)

type restorer struct {
	node     string // node name
	logger   logrus.FieldLogger
	sourcer  Sourcer
	backends OffloadBackendProvider
	schema   schemaManger
	shardSyncChan

	// TODO: keeping status in memory after restore has been done
	// is not a proper solution for communicating status to the user.
	// On app crash or restart this data will be lost
	// This should be regarded as workaround and should be fixed asap
	restoreStatusMap sync.Map
}

func newRestorer(node string, logger logrus.FieldLogger,
	sourcer Sourcer,
	backends OffloadBackendProvider,
	schema schemaManger,
) *restorer {
	return &restorer{
		node:          node,
		logger:        logger,
		sourcer:       sourcer,
		backends:      backends,
		schema:        schema,
		shardSyncChan: shardSyncChan{coordChan: make(chan interface{}, 5)},
	}
}

func (r *restorer) restore(ctx context.Context,
	req *Request,
	desc *offload.OffloadNodeDescriptor,
	store nodeStore,
) (CanCommitResponse, error) {
	expiration := req.Duration
	if expiration > _TimeoutShardCommit {
		expiration = _TimeoutShardCommit
	}
	ret := CanCommitResponse{
		Method:  OpOffload,
		ID:      req.ID,
		Timeout: expiration,
	}

	destPath := store.HomeDir()

	// make sure there is no active restore
	if prevID := r.lastOp.renew(req.ID, destPath); prevID != "" {
		err := fmt.Errorf("restore %s already in progress", prevID)
		return ret, err
	}
	r.waitingForCoordinatorToCommit.Store(true) // is set to false by wait()

	f := func() {
		var err error
		status := Status{
			Path:      destPath,
			StartedAt: time.Now().UTC(),
			Status:    offload.Transferring,
		}
		defer func() {
			status.CompletedAt = time.Now().UTC()
			if err == nil {
				status.Status = offload.Success
			} else {
				status.Err = err.Error()
				status.Status = offload.Failed
			}
			r.restoreStatusMap.Store(basePath(req.Backend, req.ID), status)
			r.lastOp.reset()
		}()

		if err = r.waitForCoordinator(expiration, req.ID); err != nil {
			r.logger.WithField("action", "create_backup").
				Error(err)
			r.lastAsyncError = err
			return
		}

		err = r.restoreAll(context.Background(), desc, store, req.NodeMapping)
		logFields := logrus.Fields{"action": "restore", "backup_id": req.ID}
		if err != nil {
			r.logger.WithFields(logFields).Error(err)
		} else {
			r.logger.WithFields(logFields).Info("backup restored successfully")
		}
	}
	enterrors.GoWrapper(f, r.logger)

	return ret, nil
}

func (r *restorer) restoreAll(ctx context.Context,
	desc *offload.OffloadNodeDescriptor,
	store nodeStore, nodeMapping map[string]string,
) (err error) {
	r.lastOp.set(offload.Transferring)
	if err := r.restoreOne(ctx, desc, store, nodeMapping); err != nil {
		return fmt.Errorf("restore id %s: %w", desc.ID, err)
	}
	r.logger.WithField("action", "restore").
		WithField("id", desc.ID).
		Info("successfully restored")
	return nil
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (r *restorer) restoreOne(ctx context.Context,
	desc *offload.OffloadNodeDescriptor,
	store nodeStore, nodeMapping map[string]string,
) (err error) {
	// classLabel := desc.Name
	// if monitoring.GetMetrics().Group {
	// 	classLabel = "n/a"
	// }
	// metric, err := monitoring.GetMetrics().BackupRestoreDurations.GetMetricWithLabelValues(getType(store.b), classLabel)
	// if err != nil {
	// 	timer := prometheus.NewTimer(metric)
	// 	defer timer.ObserveDuration()
	// }

	// TODO AL check shard exists?
	// if r.sourcer.ClassExists(desc.Name) {
	// 	return fmt.Errorf("already exists")
	// }
	fw := newFileWriter(r.sourcer, store, r.logger)

	rollback, err := fw.Write(ctx, desc)
	_ = rollback
	if err != nil {
		return fmt.Errorf("write files: %w", err)
	}
	// if err := r.schema.RestoreClass(ctx, desc, nodeMapping); err != nil {
	// 	if rerr := rollback(); rerr != nil {
	// 		r.logger.WithField("className", desc.Name).WithField("action", "rollback").Error(rerr)
	// 	}
	// 	return fmt.Errorf("restore schema: %w", err)
	// }
	return nil
}

func (r *restorer) status(backend, ID string) (Status, error) {
	if st := r.lastOp.get(); st.ID == ID {
		return Status{
			Path:      st.Path,
			StartedAt: st.StartTime,
			Status:    st.Status,
		}, nil
	}
	ref := basePath(backend, ID)
	istatus, ok := r.restoreStatusMap.Load(ref)
	if !ok {
		err := fmt.Errorf("status not found: %s", ref)
		return Status{}, backup.NewErrNotFound(err)
	}
	return istatus.(Status), nil
}

func (r *restorer) validate(ctx context.Context, store *nodeStore, req *Request) (*offload.OffloadNodeDescriptor, error) {
	destPath := store.HomeDir()
	meta, err := store.Meta(ctx)
	if err != nil {
		nerr := offload.ErrNotFound{}
		if errors.As(err, &nerr) {
			return nil, fmt.Errorf("restorer cannot validate: %w: %q (%w)", errMetaNotFound, destPath, err)
		}
		return nil, fmt.Errorf("find backup %s: %w", destPath, err)
	}
	if meta.ID != req.ID {
		return nil, fmt.Errorf("wrong backup file: expected %q got %q", req.ID, meta.ID)
	}
	if meta.Status != string(offload.Success) {
		err = fmt.Errorf("invalid backup %s status: %s", destPath, meta.Status)
		return nil, err
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("corrupted backup file: %w", err)
	}
	if v := meta.Version; v > Version {
		return nil, fmt.Errorf("%s: %s > %s", errMsgHigherVersion, v, Version)
	}
	// cs := meta.List()
	// if len(req.Classes) > 0 {
	// 	if first := meta.AllExist(req.Classes); first != "" {
	// 		err = fmt.Errorf("class %s doesn't exist in the backup, but does have %v: ", first, cs)
	// 		return nil, cs, err
	// 	}
	// 	meta.Include(req.Classes)
	// }
	return meta, nil
}
