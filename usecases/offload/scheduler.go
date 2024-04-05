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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/offload"
)

var errLocalBackend = errors.New("local filesystem backend is not viable for offloading a node cluster, try s3 or gcs")

const errMsgHigherVersion = "unable to load teant as it was produced by a higher version"

// Scheduler assigns offload operations to coordinators.
type Scheduler struct {
	// deps
	logger    logrus.FieldLogger
	offloader *offloadCoordinator
	loader    *loadCoordinator
	backends  OffloadBackendProvider
}

// NewScheduler creates a new scheduler with two coordinators
func NewScheduler(
	client client,
	selector selector,
	backends OffloadBackendProvider,
	nodeResolver nodeResolver,
	logger logrus.FieldLogger,
) *Scheduler {
	s := &Scheduler{
		logger:    logger,
		backends:  backends,
		offloader: newOffloadCoordinator(client, logger, nodeResolver, selector),
		loader:    newLoadCoordinator(client, logger, nodeResolver),
	}
	return s
}

func (s *Scheduler) Offload(ctx context.Context, oReq *OffloadRequest,
	callback func(status offload.Status),
) (err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "try_offload", oReq, begin, err)
	}(time.Now())

	store, errb := coordBackend(s.backends, oReq.Backend, oReq.ID())
	if errb != nil {
		err = fmt.Errorf("no offload backend %q: %w", oReq.Backend, errb)
		return
	}

	errv := s.validateOffloadRequest(ctx, store, oReq)
	if errv != nil {
		err = fmt.Errorf("invalid offload request: %w", errv)
		return
	}

	// TODO AL do not initialize on every request?
	if erri := store.Initialize(ctx); erri != nil {
		err = fmt.Errorf("init backend: %w", erri)
		return
	}

	req := Request{
		Method:  OpOffload,
		ID:      oReq.ID(),
		Class:   oReq.Class,
		Tenant:  oReq.Tenant,
		Backend: oReq.Backend,
	}
	if erro := s.offloader.Offload(ctx, store, &req, callback); erro != nil {
		err = fmt.Errorf("failed offload %q: %w", oReq.ID(), erro)
		return
	}
	return nil
}

func (s *Scheduler) Load(ctx context.Context, oReq *OffloadRequest,
	callback func(status offload.Status),
) (err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "try_load", oReq, begin, err)
	}(time.Now())

	store, errb := coordBackend(s.backends, oReq.Backend, oReq.ID())
	if errb != nil {
		err = fmt.Errorf("no load backend %q: %w", oReq.Backend, errb)
		return
	}

	meta, errv := s.validateLoadRequest(ctx, store, oReq)
	if errv != nil {
		err = fmt.Errorf("invalid load request: %w", errv)
		return
	}

	req := Request{
		Method:  OpLoad,
		ID:      oReq.ID(),
		Class:   oReq.Class,
		Tenant:  oReq.Tenant,
		Backend: oReq.Backend,
	}
	if errl := s.loader.Load(ctx, store, &req, meta, callback); errl != nil {
		err = fmt.Errorf("failed load %q: %w", oReq.ID(), errl)
		return
	}
	return nil
}

func coordBackend(provider OffloadBackendProvider, backend, id string) (coordStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return coordStore{}, err
	}
	return coordStore{objStore{b: caps, BasePath: id}}, nil
}

func (s *Scheduler) validateOffloadRequest(ctx context.Context, store coordStore, req *OffloadRequest) error {
	// if !store.b.IsExternal() && s.offloader.nodeResolver.NodeCount() > 1 {
	// 	return nil, errLocalBackend
	// }

	// TODO AL check whether class/tenant exist?

	destPath := store.HomeDir()
	// TODO AL remove files first?
	// there is no offload with given id on the backend, regardless of its state (valid or corrupted)
	_, err := store.Meta(ctx, OffloadMetaFile)
	if err == nil {
		return fmt.Errorf("offload %q already exists at %q", req.ID(), destPath)
	}
	// TODO AL remove backup package
	if _, ok := err.(backup.ErrNotFound); !ok {
		return fmt.Errorf("check if offload %q exists at %q: %w", req.ID(), destPath, err)
	}
	return nil
}

func (s *Scheduler) validateLoadRequest(ctx context.Context, store coordStore, req *OffloadRequest,
) (*offload.OffloadDistributedDescriptor, error) {
	// if !store.b.IsExternal() && s.loader.nodeResolver.NodeCount() > 1 {
	// 	return nil, errLocalBackend
	// }

	// TODO AL check whether class/tenant exist?

	destPath := store.HomeDir()
	meta, err := store.Meta(ctx, OffloadMetaFile)
	if err != nil {
		notFoundErr := offload.ErrNotFound{}
		if errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("offload %q does not exist: %w", req.ID(), notFoundErr)
		}
		return nil, fmt.Errorf("find offload %q at %q: %w", req.ID(), destPath, err)
	}
	if meta.ID != req.ID() {
		return nil, fmt.Errorf("wrong offload file: expected %q got %q", req.ID(), meta.ID)
	}
	if meta.Status != offload.Success {
		return nil, fmt.Errorf("invalid offload status of %q: %s", req.ID(), meta.Status)
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("corrupted offload file of %q: %w", req.ID(), err)
	}
	if v := meta.Version; v > Version {
		return nil, fmt.Errorf("%s: %s > %s", errMsgHigherVersion, v, Version)
	}

	// if len(req.NodeMapping) > 0 {
	// 	meta.NodeMapping = req.NodeMapping
	// 	meta.ApplyNodeMapping()
	// }
	return meta, nil
}

func logOperation(logger logrus.FieldLogger, action string, oReq *OffloadRequest, begin time.Time, err error) {
	le := logger.WithField("action", action).
		WithField("offload_id", oReq.ID()).
		WithField("class", oReq.Class).
		WithField("tenant", oReq.Tenant).
		WithField("backend", oReq.Backend).
		WithField("took", time.Since(begin))
	if err != nil {
		le.Error(err)
	} else {
		le.Info()
	}
}
