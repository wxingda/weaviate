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
	"sync"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/offload"
)

// Op is the kind of a offload operation
type Op string

const (
	OpOffload Op = "offload"
	OpLoad    Op = "load"
)

var (
	errTenantNotFound = errors.New("tenant not found")
	errCannotCommit   = errors.New("cannot commit")
	errMetaNotFound   = errors.New("metadata not found")
	errUnknownOp      = errors.New("unknown backup operation")
)

const (
	_BookingPeriod      = time.Second * 20
	_TimeoutNodeDown    = 7 * time.Minute
	_TimeoutQueryStatus = 5 * time.Second
	_TimeoutCanCommit   = 8 * time.Second
	_NextRoundPeriod    = 10 * time.Second
	_MaxNumberConns     = 16
	_MaxCoordWorkers    = 16
)

type nodeMap map[string]*offload.NodeDescriptor

// participantStatus tracks status of a participant in a DBRO
type participantStatus struct {
	Status   offload.Status
	LastTime time.Time
	Reason   string
}

// selector is used to select participant nodes
type selector interface {
	TenantNodes(ctx context.Context, class string, tenant string) ([]string, error)
}

type offloadCoordinator struct {
	coordinator
	selector selector
}

// newcoordinator creates an instance which coordinates distributed BRO operations among many shards.
func newOffloadCoordinator(
	selector selector,
	client client,
	log logrus.FieldLogger,
	nodeResolver nodeResolver,
) *offloadCoordinator {
	c := &offloadCoordinator{
		selector: selector,
		coordinator: coordinator{
			client:             client,
			log:                log,
			nodeResolver:       nodeResolver,
			ops:                newOperations(),
			timeoutNodeDown:    _TimeoutNodeDown,
			timeoutQueryStatus: _TimeoutQueryStatus,
			timeoutCanCommit:   _TimeoutCanCommit,
			timeoutNextRound:   _NextRoundPeriod,
		},
	}
	c.startWorkers(_MaxCoordWorkers, c.handleJob)

	return c
}

// Backup coordinates a distributed backup among participants
func (c *offloadCoordinator) Offload(ctx context.Context, store coordStore,
	req *Request, callback func(status offload.Status),
) error {
	// make sure there is no same active offload
	if _, exists := c.ops.renew(req.ID, store.HomeDir()); exists {
		return fmt.Errorf("offload %s already in progress", req.ID)
	}

	nodes, err := c.groupByNode(ctx, req.Class, req.Tenant)
	if err != nil {
		return err
	}

	descriptor := &offload.OffloadDistributedDescriptor{
		ID:            req.ID,
		Class:         req.Class,
		Tenant:        req.Tenant,
		StartedAt:     time.Now().UTC(),
		Status:        offload.Started,
		Nodes:         nodes,
		Version:       Version,
		ServerVersion: config.ServerVersion,
	}

	errCh := make(chan error, 1)
	c.jobsCh <- job{
		descriptor: descriptor,
		req:        req,
		store:      store,
		ctx:        ctx,
		errCh:      errCh,
		callback:   callback,
	}

	return <-errCh
}

func (c *offloadCoordinator) groupByNode(ctx context.Context, class string, tenant string) (nodeMap, error) {
	m := make(nodeMap, 8)

	nodes, err := c.selector.TenantNodes(ctx, class, tenant)
	if err != nil {
		return nil, fmt.Errorf("class %q, tenant %q: %w", class, tenant, errTenantNotFound)
	}

	for _, node := range nodes {
		m[node] = &offload.NodeDescriptor{}
	}
	return m, nil
}

func (c *offloadCoordinator) handleJob(j job) {
	j.errCh <- func() error {
		hosts, err := c.canCommit(j.ctx, j.req, j.descriptor)
		if err != nil {
			c.ops.reset(j.req.ID)
			return err
		}

		if err := j.store.PutMeta(j.ctx, OffloadFile, j.descriptor); err != nil {
			c.ops.reset(j.req.ID)
			return fmt.Errorf("cannot init meta file: %w", err)
		}

		statusReq := StatusRequest{
			Method:  OpOffload,
			ID:      j.req.ID,
			Backend: j.req.Backend,
		}

		f := func() {
			defer c.ops.reset(j.req.ID)
			ctx := context.Background()

			c.commit(ctx, &statusReq, hosts, j.descriptor)
			logFields := logrus.Fields{
				"action":     OpOffload,
				"offload_id": j.req.ID,
				"class":      j.req.Class,
				"tenant":     j.req.Tenant,
			}
			if err := j.store.PutMeta(ctx, OffloadFile, j.descriptor); err != nil {
				c.log.WithFields(logFields).Errorf("coordinator: put_meta: %v", err)
			}
			if j.descriptor.Status == offload.Success {
				c.log.WithFields(logFields).Info("coordinator: offload completed successfully")
			} else {
				c.log.WithFields(logFields).Errorf("coordinator: %s", j.descriptor.Error)
			}
			j.callback(j.descriptor.Status)
		}
		enterrors.GoWrapper(f, c.log)
		return nil
	}()
	close(j.errCh)
}

type loadCoordinator struct {
	coordinator
}

// newcoordinator creates an instance which coordinates distributed BRO operations among many shards.
func newLoadCoordinator(
	client client,
	log logrus.FieldLogger,
	nodeResolver nodeResolver,
) *loadCoordinator {
	c := &loadCoordinator{
		coordinator: coordinator{
			client:             client,
			log:                log,
			nodeResolver:       nodeResolver,
			ops:                newOperations(),
			timeoutNodeDown:    _TimeoutNodeDown,
			timeoutQueryStatus: _TimeoutQueryStatus,
			timeoutCanCommit:   _TimeoutCanCommit,
			timeoutNextRound:   _NextRoundPeriod,
		},
	}
	c.startWorkers(_MaxCoordWorkers, c.handleJob)

	return c
}

// Restore coordinates a distributed restoration among participants
func (c *loadCoordinator) Load(ctx context.Context, store coordStore, req *Request,
	desc *offload.OffloadDistributedDescriptor, callback func(status offload.Status),
) error {
	// make sure there is no same active load
	if _, exists := c.ops.renew(req.ID, store.HomeDir()); exists {
		return fmt.Errorf("load %s already in progress", req.ID)
	}

	descriptor := desc.ResetStatus()

	errCh := make(chan error, 1)
	c.jobsCh <- job{
		descriptor: descriptor,
		req:        req,
		store:      store,
		ctx:        ctx,
		errCh:      errCh,
		callback:   callback,
	}
	return <-errCh
}

func (c *coordinator) handleJob(j job) {
	j.errCh <- func() error {
		nodes, err := c.canCommit(j.ctx, j.req, j.descriptor)
		if err != nil {
			c.ops.reset(j.req.ID)
			return err
		}

		// initial put so restore status is immediately available
		if err := j.store.PutMeta(j.ctx, LoadFile, j.descriptor); err != nil {
			c.ops.reset(j.req.ID)
			req := &AbortRequest{Method: OpLoad, ID: j.req.ID, Backend: j.req.Backend}
			c.abortAll(j.ctx, req, nodes)
			return fmt.Errorf("put initial metadata: %w", err)
		}

		statusReq := StatusRequest{Method: OpLoad, ID: j.req.ID, Backend: j.req.Backend}
		g := func() {
			defer c.ops.reset(j.req.ID)
			ctx := context.Background()

			c.commit(ctx, &statusReq, nodes, j.descriptor)
			logFields := logrus.Fields{
				"action":  OpLoad,
				"load_id": j.req.ID,
				"class":   j.req.Class,
				"tenant":  j.req.Tenant,
			}
			if err := j.store.PutMeta(ctx, LoadFile, j.descriptor); err != nil {
				c.log.WithFields(logFields).Errorf("coordinator: put_meta: %v", err)
			}
			if j.descriptor.Status == offload.Success {
				c.log.WithFields(logFields).Info("coordinator: backup restored successfully")
			} else {
				c.log.WithFields(logFields).Errorf("coordinator: %v", j.descriptor.Error)
			}
			j.callback(j.descriptor.Status)
		}
		enterrors.GoWrapper(g, c.log)
		return nil
	}()
	close(j.errCh)
}

// coordinator coordinates a distributed backup and restore operation (DBRO):
//
// - It determines what request to send to which shard.
//
// - I will return an error, If any shards refuses to participate in DBRO.
//
// - It keeps all metadata needed to resume a DBRO in an external storage (e.g. s3).
//
// - When it starts it will check for any broken DBROs using its metadata.
//
// - It can resume a broken a DBRO
//
// - It marks the whole DBRO as failed if any shard fails to do its BRO.
//
// - The coordinator will try to repair previous DBROs whenever it is possible
type coordinator struct {
	// dependencies
	client       client
	log          logrus.FieldLogger
	nodeResolver nodeResolver

	// state
	jobsCh chan job
	ops    *operations

	// timeouts
	timeoutNodeDown    time.Duration
	timeoutQueryStatus time.Duration
	timeoutCanCommit   time.Duration
	timeoutNextRound   time.Duration
}

func (c *coordinator) startWorkers(maxWorkers int, handler func(j job)) {
	c.jobsCh = make(chan job, maxWorkers*2)

	enterrors.GoWrapper(func() {
		eg := enterrors.NewErrorGroupWrapper(c.log)
		eg.SetLimit(maxWorkers)

		for j := range c.jobsCh {
			eg.Go(func() error {
				handler(j)
				return nil
			})
		}
	}, c.log)
}

// canCommit asks candidates if they agree to participate in DBRO
// It returns and error if any candidates refuses to participate
func (c *coordinator) canCommit(ctx context.Context, req *Request,
	descriptor *offload.OffloadDistributedDescriptor,
) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutCanCommit)
	defer cancel()

	type nodeHost struct {
		node, host string
	}

	type pair struct {
		n nodeHost
		r *Request
	}

	nodeMapping := descriptor.NodeMapping
	groups := descriptor.Nodes

	g, ctx := enterrors.NewErrorGroupWithContextWrapper(c.log, ctx)
	g.SetLimit(_MaxNumberConns)
	reqChan := make(chan pair)
	g.Go(func() error {
		defer close(reqChan)
		for node := range groups {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// If we have a nodeMapping with the node name from the backup, replace the node with the new one
			node = descriptor.ToMappedNodeName(node)

			host, found := c.nodeResolver.NodeHostname(node)
			if !found {
				return fmt.Errorf("cannot resolve hostname for %q", node)
			}

			reqChan <- pair{
				nodeHost{node, host},
				&Request{
					Method:      req.Method,
					ID:          req.ID,
					Backend:     req.Backend,
					Class:       req.Class,
					Tenant:      req.Tenant,
					Duration:    _BookingPeriod,
					NodeMapping: nodeMapping,
				},
			}
		}
		return nil
	})

	mutex := sync.RWMutex{}
	nodes := make(map[string]string, len(groups))
	for pair := range reqChan {
		pair := pair
		g.Go(func() error {
			resp, err := c.client.CanCommit(ctx, pair.n.host, pair.r)
			if err == nil && resp.Timeout == 0 {
				err = fmt.Errorf("%w : %v", errCannotCommit, resp.Err)
			}
			if err != nil {
				return fmt.Errorf("node %q: %w", pair.n, err)
			}
			mutex.Lock()
			nodes[pair.n.node] = pair.n.host
			mutex.Unlock()
			return nil
		})
	}
	abortReq := &AbortRequest{Method: req.Method, ID: req.ID, Backend: req.Backend}
	if err := g.Wait(); err != nil {
		c.abortAll(ctx, abortReq, nodes)
		return nil, err
	}
	return nodes, nil
}

// commit tells each participant to commit its backup operation
// It stores the final result in the provided backend
func (c *coordinator) commit(ctx context.Context,
	req *StatusRequest, node2Addr map[string]string,
	descriptor *offload.OffloadDistributedDescriptor,
) {
	participants := make(map[string]participantStatus)
	// create a new copy for commitAll and queryAll to mutate
	node2Host := make(map[string]string, len(node2Addr))
	for k, v := range node2Addr {
		node2Host[k] = v
	}
	nFailures := c.commitAll(ctx, req, node2Host, participants)
	retryAfter := c.timeoutNextRound / 5 // 2s for first time
	canContinue := len(node2Host) > 0 && nFailures == 0
	for canContinue {
		<-time.After(retryAfter)
		retryAfter = c.timeoutNextRound
		nFailures += c.queryAll(ctx, req, node2Host, participants)
		canContinue = len(node2Host) > 0 && nFailures == 0
	}
	if nFailures > 0 {
		req := &AbortRequest{Method: req.Method, ID: req.ID, Backend: req.Backend}
		c.abortAll(context.Background(), req, node2Addr)
	}
	descriptor.CompletedAt = time.Now().UTC()
	status := offload.Success
	reason := ""
	groups := descriptor.Nodes
	for node, p := range participants {
		st := groups[descriptor.ToOriginalNodeName(node)]
		st.Status, st.Error = p.Status, p.Reason
		if p.Status != offload.Success {
			status = offload.Failed
			reason = p.Reason
		}
		groups[node] = st
	}
	descriptor.Status = status
	descriptor.Error = reason
}

// queryAll queries all participant and store their statuses internally
//
// It returns the number of failed node backups
func (c *coordinator) queryAll(ctx context.Context, req *StatusRequest,
	nodes map[string]string, participants map[string]participantStatus,
) int {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutQueryStatus)
	defer cancel()

	rs := make([]partialStatus, len(nodes))
	g, ctx := enterrors.NewErrorGroupWithContextWrapper(c.log, ctx)
	g.SetLimit(_MaxNumberConns)
	i := 0
	for node, hostname := range nodes {
		j := i
		hostname := hostname
		rs[j].node = node
		g.Go(func() error {
			rs[j].StatusResponse, rs[j].err = c.client.Status(ctx, hostname, req)
			return nil
		})
		i++
	}
	g.Wait()
	n, now := 0, time.Now()
	for _, r := range rs {
		st := participants[r.node]
		if r.err == nil {
			st.LastTime, st.Status, st.Reason = now, r.Status, r.Err
			if r.Status == offload.Success {
				delete(nodes, r.node)
			}
			if r.Status == offload.Failed {
				delete(nodes, r.node)
				n++
			}
		} else if now.Sub(st.LastTime) > c.timeoutNodeDown {
			n++
			st.Status = offload.Failed
			st.Reason = fmt.Sprintf("node %q might be down: %v", r.node, r.err.Error())
			delete(nodes, r.node)
		}
		participants[r.node] = st
	}
	return n
}

// commitAll tells all participants to proceed with their backup operations
// It returns the number of failures
func (c *coordinator) commitAll(ctx context.Context, req *StatusRequest,
	nodes map[string]string, participants map[string]participantStatus,
) int {
	type pair struct {
		node string
		err  error
	}
	errChan := make(chan pair)
	aCounter := int64(len(nodes))
	g, ctx := enterrors.NewErrorGroupWithContextWrapper(c.log, ctx)
	g.SetLimit(_MaxNumberConns)
	for node, hostname := range nodes {
		node, hostname := node, hostname
		g.Go(func() error {
			defer func() {
				if atomic.AddInt64(&aCounter, -1) == 0 {
					close(errChan)
				}
			}()
			err := c.client.Commit(ctx, hostname, req)
			if err != nil {
				errChan <- pair{node, err}
			}
			return nil
		})
	}
	nFailures := 0
	for x := range errChan {
		st := participants[x.node]
		st.Status = offload.Failed
		st.Reason = "might be down:" + x.err.Error()
		participants[x.node] = st
		c.log.WithField("action", req.Method).
			WithField("backup_id", req.ID).
			WithField("node", x.node).Error(x.err)
		delete(nodes, x.node)
		nFailures++
		continue
	}
	return nFailures
}

// abortAll tells every node to abort transaction
func (c *coordinator) abortAll(ctx context.Context, req *AbortRequest, nodes map[string]string) {
	for name, hostname := range nodes {
		if err := c.client.Abort(ctx, hostname, req); err != nil {
			c.log.WithField("action", req.Method).
				WithField("backup_id", req.ID).
				WithField("node", name).Errorf("abort %v", err)
		}
	}
}

type operations struct {
	m map[string]reqStat
	sync.Mutex
}

func newOperations() *operations {
	return &operations{m: make(map[string]reqStat)}
}

func (o *operations) renew(id string, path string) (reqStat, bool) {
	o.Lock()
	defer o.Unlock()

	if rs, ok := o.m[id]; ok {
		return rs, true
	}

	// TODO AL verify if all fields needed
	rs := reqStat{
		ID:        id,
		Path:      path,
		StartTime: time.Now().UTC(),
		Status:    offload.Started,
	}
	o.m[id] = rs
	return rs, false
}

func (o *operations) reset(id string) {
	o.Lock()
	defer o.Unlock()

	delete(o.m, id)
}

type job struct {
	descriptor *offload.OffloadDistributedDescriptor
	req        *Request
	store      coordStore
	ctx        context.Context
	errCh      chan<- error
	callback   func(status offload.Status) // TODO AL verify argument
}

// partialStatus tracks status of a single backup operation
type partialStatus struct {
	node string
	*StatusResponse
	err error
}
