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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/offload"
)

var (
	errLocalBackendDBRO = errors.New("local filesystem backend is not viable for backing up a node cluster, try s3 or gcs")
	errIncludeExclude   = errors.New("malformed request: 'include' and 'exclude' cannot both contain values")
)

const (
	errMsgHigherVersion = "unable to restore backup as it was produced by a higher version"
)

// Scheduler assigns backup operations to coordinators.
type Scheduler struct {
	// deps
	logger     logrus.FieldLogger
	authorizer authorizer
	offloader  *coordinator
	onloader   *coordinator
	backends   OffloadBackendProvider
}

// NewScheduler creates a new scheduler with two coordinators
func NewScheduler(
	authorizer authorizer,
	client client,
	sourcer selector,
	backends OffloadBackendProvider,
	nodeResolver nodeResolver,
	logger logrus.FieldLogger,
) *Scheduler {
	m := &Scheduler{
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		offloader: newCoordinator(
			sourcer,
			client,
			logger, nodeResolver),
		onloader: newCoordinator(
			sourcer,
			client,
			logger, nodeResolver),
	}
	return m
}

func (s *Scheduler) Offload(ctx context.Context, pr *models.Principal, req *OffloadRequest,
) (_ *models.OffloadResponse, err error) {
	// defer func(begin time.Time) {
	// 	logOperation(s.logger, "try_backup", req.ID, req.Backend, begin, err)
	// }(time.Now())

	// TODO authorize
	path := fmt.Sprintf("offloads/%s/%s", req.Backend, req.Class)
	if err := s.authorizer.Authorize(pr, "add", path); err != nil {
		return nil, err
	}
	store, err := coordBackend(s.backends, req.Backend, req.ID())
	if err != nil {
		err = fmt.Errorf("no offloads backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, offload.NewErrUnprocessable(err)
	}

	// classes, err := s.validateOffloadRequest(ctx, store, req)
	// if err != nil {
	// 	return nil, offload.NewErrUnprocessable(err)
	// }

	if err := store.Initialize(ctx); err != nil {
		return nil, offload.NewErrUnprocessable(fmt.Errorf("init backend: %w", err))
	}
	breq := Request{
		Method:      OpCreate,
		Backend:     req.Backend,
		Class:       req.Class,
		Tenant:      req.Tenant,
		Compression: req.Compression,
		ID:          req.ID(),
	}
	if err := s.offloader.Offload(ctx, store, &breq, func(_ offload.Status) {}); err != nil {
		return nil, offload.NewErrUnprocessable(err)
	} else {
		return nil, nil
		// st := s.offloader.lastOp.get()
		// status := string(st.Status)
		// return &models.OffloadResponse{
		// 	Class:   req.Class,
		// 	Tenant:  req.Tenant,
		// 	Backend: req.Backend,
		// 	Status:  &status,
		// 	Path:    st.Path,
		// }, nil
	}
}

func (s *Scheduler) OffloadSimple(ctx context.Context, req *OffloadRequest,
	callback func(status offload.Status),
) error {
	// defer func(begin time.Time) {
	// 	logOperation(s.logger, "try_backup", req.ID, req.Backend, begin, err)
	// }(time.Now())

	// // TODO authorize
	// path := fmt.Sprintf("offloads/%s/%s", req.Backend, req.Class)
	// if err := s.authorizer.Authorize(pr, "add", path); err != nil {
	// 	return nil, err
	// }
	store, err := coordBackend(s.backends, req.Backend, req.ID())
	if err != nil {
		// err = fmt.Errorf("no offloads backend %q: %w, did you enable the right module?", req.Backend, err)
		// return nil, offload.NewErrUnprocessable(err)
		return fmt.Errorf("no offloads backend %q: %w, did you enable the right module?", req.Backend, err)
	}

	// classes, err := s.validateOffloadRequest(ctx, store, req)
	// if err != nil {
	// 	return nil, offload.NewErrUnprocessable(err)
	// }

	if err := store.Initialize(ctx); err != nil {
		return fmt.Errorf("init backend: %w", err)
		// return nil, offload.NewErrUnprocessable(fmt.Errorf("init backend: %w", err))
	}
	breq := Request{
		Method:      OpCreate,
		Backend:     req.Backend,
		Class:       req.Class,
		Tenant:      req.Tenant,
		Compression: req.Compression,
		ID:          req.ID(),
	}
	if err := s.offloader.Offload(ctx, store, &breq, callback); err != nil {
		return offload.NewErrUnprocessable(err)
	} else {
		return nil
		// st := s.offloader.lastOp.get()
		// status := string(st.Status)
		// return &models.OffloadResponse{
		// 	Class:   req.Class,
		// 	Tenant:  req.Tenant,
		// 	Backend: req.Backend,
		// 	Status:  &status,
		// 	Path:    st.Path,
		// }, nil
	}
}

func (s *Scheduler) Onload(ctx context.Context, pr *models.Principal,
	req *OffloadRequest,
) (_ *models.OnloadResponse, err error) {
	// defer func(begin time.Time) {
	// 	logOperation(s.logger, "try_restore", req.ID, req.Backend, begin, err)
	// }(time.Now())

	// TODO authorize
	path := fmt.Sprintf("offloads/%s/%s/onload", req.Backend, req.Class)
	if err := s.authorizer.Authorize(pr, "restore", path); err != nil {
		return nil, err
	}
	store, err := coordBackend(s.backends, req.Backend, req.ID())
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, offload.NewErrUnprocessable(err)
	}
	meta, err := s.validateRestoreRequest(ctx, store, req)
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, offload.NewErrNotFound(err)
		}
		return nil, offload.NewErrUnprocessable(err)
	}
	status := string(offload.Started)
	data := &models.OnloadResponse{
		Backend: req.Backend,
		Path:    store.HomeDir(),
		Class:   req.Class,
		Tenant:  req.Tenant,
	}

	rReq := Request{
		Method:      OpRestore,
		ID:          req.ID(),
		Backend:     req.Backend,
		Compression: req.Compression,
		Class:       req.Class,
		Tenant:      req.Tenant,
	}
	err = s.onloader.Onload(ctx, store, &rReq, meta, func(status offload.Status) {})
	if err != nil {
		status = string(offload.Failed)
		data.Error = err.Error()
		return nil, offload.NewErrUnprocessable(err)
	}

	data.Status = &status
	return data, nil
}

func (s *Scheduler) OnloadSimple(ctx context.Context, req *OffloadRequest,
	callback func(status offload.Status),
) error {
	// defer func(begin time.Time) {
	// 	logOperation(s.logger, "try_restore", req.ID, req.Backend, begin, err)
	// }(time.Now())

	// // TODO authorize
	// path := fmt.Sprintf("offloads/%s/%s/onload", req.Backend, req.Class)
	// if err := s.authorizer.Authorize(pr, "restore", path); err != nil {
	// 	return nil, err
	// }
	store, err := coordBackend(s.backends, req.Backend, req.ID())
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return offload.NewErrUnprocessable(err)
	}
	meta, err := s.validateRestoreRequest(ctx, store, req)
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return offload.NewErrNotFound(err)
		}
		return offload.NewErrUnprocessable(err)
	}
	// status := string(offload.Started)
	// data := &models.OnloadResponse{
	// 	Backend: req.Backend,
	// 	Path:    store.HomeDir(),
	// 	Class:   req.Class,
	// 	Tenant:  req.Tenant,
	// }

	rReq := Request{
		Method:      OpRestore,
		ID:          req.ID(),
		Backend:     req.Backend,
		Compression: req.Compression,
		Class:       req.Class,
		Tenant:      req.Tenant,
	}
	err = s.onloader.Onload(ctx, store, &rReq, meta, callback)
	if err != nil {
		// status = string(offload.Failed)
		// data.Error = err.Error()
		return offload.NewErrUnprocessable(err)
	}

	// data.Status = &status
	return nil
}

// func (s *Scheduler) BackupStatus(ctx context.Context, principal *models.Principal,
// 	backend, backupID string,
// ) (_ *Status, err error) {
// 	defer func(begin time.Time) {
// 		logOperation(s.logger, "backup_status", backupID, backend, begin, err)
// 	}(time.Now())
// 	path := fmt.Sprintf("backups/%s/%s", backend, backupID)
// 	if err := s.authorizer.Authorize(principal, "get", path); err != nil {
// 		return nil, err
// 	}
// 	store, err := coordBackend(s.backends, backend, backupID)
// 	if err != nil {
// 		err = fmt.Errorf("no backup provider %q: %w, did you enable the right module?", backend, err)
// 		return nil, backup.NewErrUnprocessable(err)
// 	}

// 	req := &StatusRequest{OpCreate, backupID, backend}
// 	st, err := s.offloader.OnStatus(ctx, store, req)
// 	if err != nil {
// 		return nil, backup.NewErrNotFound(err)
// 	}
// 	return st, nil
// }

// func (s *Scheduler) RestorationStatus(ctx context.Context, principal *models.Principal, backend, backupID string,
// ) (_ *Status, err error) {
// 	defer func(begin time.Time) {
// 		logOperation(s.logger, "restoration_status", backupID, backend, time.Now(), err)
// 	}(time.Now())
// 	path := fmt.Sprintf("backups/%s/%s/restore", backend, backupID)
// 	if err := s.authorizer.Authorize(principal, "get", path); err != nil {
// 		return nil, err
// 	}
// 	store, err := coordBackend(s.backends, backend, backupID)
// 	if err != nil {
// 		err = fmt.Errorf("no backup provider %q: %w, did you enable the right module?", backend, err)
// 		return nil, backup.NewErrUnprocessable(err)
// 	}
// 	req := &StatusRequest{OpRestore, backupID, backend}
// 	st, err := s.onloader.OnStatus(ctx, store, req)
// 	if err != nil {
// 		return nil, backup.NewErrNotFound(err)
// 	}
// 	return st, nil
// }

func coordBackend(provider OffloadBackendProvider, backend, id string) (coordStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return coordStore{}, err
	}
	return coordStore{objStore{b: caps, BasePath: id}}, nil
}

// func (s *Scheduler) validateOffloadRequest(ctx context.Context, store coordStore, req *OffloadRequest) ([]string, error) {
// 	// if !store.b.IsExternal() && s.backupper.nodeResolver.NodeCount() > 1 {
// 	// 	return nil, errLocalBackendDBRO
// 	// }

// 	if err := validateID(req.ID); err != nil {
// 		return nil, err
// 	}
// 	if len(req.Include) > 0 && len(req.Exclude) > 0 {
// 		return nil, errIncludeExclude
// 	}
// 	if dup := findDuplicate(req.Include); dup != "" {
// 		return nil, fmt.Errorf("class list 'include' contains duplicate: %s", dup)
// 	}
// 	classes := req.Include
// 	if len(classes) == 0 {
// 		classes = s.offloader.selector.ListClasses(ctx)
// 		// no classes exist in the DB
// 		if len(classes) == 0 {
// 			return nil, fmt.Errorf("no available classes to backup, there's nothing to do here")
// 		}
// 	}
// 	if classes = filterClasses(classes, req.Exclude); len(classes) == 0 {
// 		return nil, fmt.Errorf("empty class list: please choose from : %v", classes)
// 	}

// 	if err := s.offloader.selector.Backupable(ctx, classes); err != nil {
// 		return nil, err
// 	}
// 	destPath := store.HomeDir()
// 	// there is no backup with given id on the backend, regardless of its state (valid or corrupted)
// 	_, err := store.Meta(ctx, GlobalBackupFile)
// 	if err == nil {
// 		return nil, fmt.Errorf("backup %q already exists at %q", req.ID, destPath)
// 	}
// 	if _, ok := err.(backup.ErrNotFound); !ok {
// 		return nil, fmt.Errorf("check if backup %q exists at %q: %w", req.ID, destPath, err)
// 	}
// 	return classes, nil
// }

func (s *Scheduler) validateRestoreRequest(ctx context.Context, store coordStore, req *OffloadRequest) (*offload.OffloadDistributedDescriptor, error) {
	// if !store.b.IsExternal() && s.restorer.nodeResolver.NodeCount() > 1 {
	// 	return nil, errLocalBackendDBRO
	// }
	// if len(req.Include) > 0 && len(req.Exclude) > 0 {
	// 	return nil, errIncludeExclude
	// }
	// if dup := findDuplicate(req.Include); dup != "" {
	// 	return nil, fmt.Errorf("class list 'include' contains duplicate: %s", dup)
	// }
	destPath := store.HomeDir()
	meta, err := store.Meta(ctx, GlobalOffloadFile)
	if err != nil {
		notFoundErr := offload.ErrNotFound{}
		if errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("backup id %q %q does not exist: %v: %w", req.Class, req.Tenant, notFoundErr, errMetaNotFound)
		}
		return nil, fmt.Errorf("find backup %s: %w", destPath, err)
	}
	if meta.ID != req.ID() {
		return nil, fmt.Errorf("wrong backup file: expected %q got %q", req.ID, meta.ID)
	}
	if meta.Status != offload.Success {
		return nil, fmt.Errorf("invalid backup %s status: %s", destPath, meta.Status)
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("corrupted backup file: %w", err)
	}
	if v := meta.Version; v > Version {
		return nil, fmt.Errorf("%s: %s > %s", errMsgHigherVersion, v, Version)
	}
	// cs := meta.Classes()
	// if len(req.Include) > 0 {
	// 	if first := meta.AllExist(req.Include); first != "" {
	// 		err = fmt.Errorf("class %s doesn't exist in the backup, but does have %v: ", first, cs)
	// 		return nil, err
	// 	}
	// 	meta.Include(req.Include)
	// } else {
	// 	meta.Exclude(req.Exclude)
	// }
	// if meta.RemoveEmpty().Count() == 0 {
	// 	return nil, fmt.Errorf("nothing left to restore: please choose from : %v", cs)
	// }
	if len(req.NodeMapping) > 0 {
		meta.NodeMapping = req.NodeMapping
		// meta.ApplyNodeMapping()
	}
	return meta, nil
}

func logOperation(logger logrus.FieldLogger, name, id, backend string, begin time.Time, err error) {
	le := logger.WithField("action", name).
		WithField("backup_id", id).WithField("backend", backend).
		WithField("took", time.Since(begin))
	if err != nil {
		le.Error(err)
	} else {
		le.Info()
	}
}

// findDuplicate returns first duplicate if it is found, and "" otherwise
func findDuplicate(xs []string) string {
	m := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		if _, ok := m[x]; ok {
			return x
		}
		m[x] = struct{}{}
	}
	return ""
}
