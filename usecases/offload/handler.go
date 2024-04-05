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
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/offload"
)

// Version of offload structure
const Version = "1.0"

type OffloadBackendProvider interface {
	BackupBackend(backend string) (modulecapabilities.BackupBackend, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
	NodeCount() int
	LocalName() string
}

type Status struct {
	Path        string
	StartedAt   time.Time
	CompletedAt time.Time
	Status      offload.Status
	Err         string
}

type Handler struct {
	node string
	// deps
	logger     logrus.FieldLogger
	authorizer authorizer
	backupper  *backupper
	restorer   *restorer
	backends   OffloadBackendProvider
}

func NewHandler(
	logger logrus.FieldLogger,
	authorizer authorizer,
	nodeResolver nodeResolver,
	sourcer Sourcer,
	backends OffloadBackendProvider,
) *Handler {
	node := nodeResolver.LocalName()
	m := &Handler{
		node:       node,
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		backupper:  newBackupper(node, logger, sourcer, backends),
		restorer:   newRestorer(node, logger, sourcer, backends),
	}
	return m
}

// Compression is the compression configuration.
type Compression struct {
	// Level is one of DefaultCompression, BestSpeed, BestCompression
	Level CompressionLevel

	// ChunkSize represents the desired size for chunks between 1 - 512  MB
	// However, during compression, the chunk size might
	// slightly deviate from this value, being either slightly
	// below or above the specified size
	ChunkSize int

	// CPUPercentage desired CPU core utilization (1%-80%), default: 50%
	CPUPercentage int
}

// OffloadRequest a transition request from API to Backend.
type OffloadRequest struct {
	// Backend specify on which backend to store backups (gcs, s3, ..)
	Backend string

	Class string

	Tenant string

	// NodeMapping is a map of node name replacement where key is the old name and value is the new name
	// No effect if the map is empty
	NodeMapping map[string]string
}

func (r *OffloadRequest) ID() string {
	return fmt.Sprintf("%s/%s", r.Class, r.Tenant)
}

// OnCanCommit will be triggered when coordinator asks the node to participate
// in a distributed offload operation
func (m *Handler) OnCanCommit(ctx context.Context, req *Request) *CanCommitResponse {
	resp := &CanCommitResponse{Method: req.Method, ID: req.ID}

	nodeName := m.node
	// If we are doing a load and have a nodeMapping specified,
	// ensure we use the "old" node name from the offload to retrieve/store the
	// offload information.
	if req.Method == OpLoad {
		for oldNodeName, newNodeName := range req.NodeMapping {
			if nodeName == newNodeName {
				nodeName = oldNodeName
				break
			}
		}
	}
	store, err := nodeBackend(nodeName, m.backends, req.Backend, req.ID)
	if err != nil {
		resp.Err = fmt.Sprintf("no offload backend %q", req.Backend)
		return resp
	}

	switch req.Method {
	case OpOffload:
		// TODO AL check whether class/tenant exist?
		// TODO AL do not initialize on every request?
		if err = store.Initialize(ctx); err != nil {
			resp.Err = fmt.Sprintf("init backend: %v", err)
			return resp
		}
		res, err := m.backupper.backup(ctx, store, req)
		if err != nil {
			resp.Err = err.Error()
			return resp
		}
		resp.Timeout = res.Timeout
	case OpLoad:
		meta, err := m.restorer.validate(ctx, &store, req)
		if err != nil {
			resp.Err = err.Error()
			return resp
		}
		res, err := m.restorer.restore(ctx, req, meta, store)
		if err != nil {
			resp.Err = err.Error()
			return resp
		}
		resp.Timeout = res.Timeout
	default:
		resp.Err = fmt.Sprintf("unknown backup operation: %s", req.Method)
		return resp
	}

	return resp
}

// OnCommit will be triggered when the coordinator confirms the execution of a previous operation
func (m *Handler) OnCommit(ctx context.Context, req *StatusRequest) (err error) {
	switch req.Method {
	case OpOffload:
		return m.backupper.OnCommit(ctx, req)
	case OpLoad:
		return m.restorer.OnCommit(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)
	}
}

// OnAbort will be triggered when the coordinator abort the execution of a previous operation
func (m *Handler) OnAbort(ctx context.Context, req *AbortRequest) error {
	switch req.Method {
	case OpOffload:
		return m.backupper.OnAbort(ctx, req)
	case OpLoad:
		return m.restorer.OnAbort(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)

	}
}

func (m *Handler) OnStatus(ctx context.Context, req *StatusRequest) *StatusResponse {
	ret := StatusResponse{
		Method: req.Method,
		ID:     req.ID,
	}
	switch req.Method {
	case OpOffload:
		st, err := m.backupper.OnStatus(ctx, req)
		ret.Status = st.Status
		if err != nil {
			ret.Status = offload.Failed
			ret.Err = err.Error()
		}
	case OpLoad:
		st, err := m.restorer.status(req.Backend, req.ID)
		ret.Status = st.Status
		ret.Err = st.Err
		if err != nil {
			ret.Status = offload.Failed
			ret.Err = err.Error()
		} else if st.Err != "" {
			ret.Err = st.Err
		}
	default:
		ret.Status = offload.Failed
		ret.Err = fmt.Sprintf("%v: %s", errUnknownOp, req.Method)
	}

	return &ret
}

// func validateID(backupID string) error {
// 	if !regExpID.MatchString(backupID) {
// 		return fmt.Errorf("invalid backup id: allowed characters are lowercase, 0-9, _, -")
// 	}
// 	return nil
// }

func nodeBackend(node string, provider OffloadBackendProvider, backend, id string) (nodeStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return nodeStore{}, err
	}
	return nodeStore{objStore{b: caps, BasePath: fmt.Sprintf("%s/%s", id, node)}}, nil
}

// basePath of the backup
func basePath(backendType, backupID string) string {
	return fmt.Sprintf("%s/%s", backendType, backupID)
}
