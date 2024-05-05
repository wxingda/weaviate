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

package store

import (
	"container/heap"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	errClassNotFound = errors.New("class not found")
	errClassExists   = errors.New("class already exists")
	errShardNotFound = errors.New("shard not found")
)

type ClassInfo struct {
	Exists            bool
	MultiTenancy      models.MultiTenancyConfig
	ReplicationFactor int
	Tenants           int
	Properties        int
	ClassVersion      uint64
	ShardVersion      uint64
}

func (ci *ClassInfo) Version() uint64 {
	return max(ci.ClassVersion, ci.ShardVersion)
}

type schema struct {
	nodeID      string
	shardReader shardReader
	sync.RWMutex
	Classes map[string]*metaClass
}

func (s *schema) ClassInfo(class string) ClassInfo {
	s.RLock()
	defer s.RUnlock()
	cl, ok := s.Classes[class]
	if !ok {
		return ClassInfo{}
	}
	return cl.ClassInfo()
}

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (s *schema) ClassEqual(name string) string {
	s.RLock()
	defer s.RUnlock()
	for k := range s.Classes {
		if strings.EqualFold(k, name) {
			return k
		}
	}
	return ""
}

func (s *schema) MultiTenancy(class string) models.MultiTenancyConfig {
	mtc, _ := s.metaClass(class).MultiTenancyConfig()
	return mtc
}

// Read performs a read operation `reader` on the specified class and sharding state
func (s *schema) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	meta := s.metaClass(class)
	if meta == nil {
		return errClassNotFound
	}
	return meta.RLockGuard(reader)
}

func (s *schema) metaClass(class string) *metaClass {
	s.RLock()
	defer s.RUnlock()
	return s.Classes[class]
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (s *schema) ReadOnlyClass(class string) (*models.Class, uint64) {
	s.RLock()
	defer s.RUnlock()
	meta := s.Classes[class]
	if meta == nil {
		return nil, 0
	}
	return meta.CloneClass(), meta.ClassVersion
}

// ReadOnlySchema returns a read only schema
// Changing the schema outside this package might lead to undefined behavior.
//
// it creates a shallow copy of existing classes
//
// This function assumes that class attributes are being overwritten.
// The properties attribute is the only one that might vary in size;
// therefore, we perform a shallow copy of the existing properties.
// This implementation assumes that individual properties are overwritten rather than partially updated
func (s *schema) ReadOnlySchema() models.Schema {
	cp := models.Schema{}
	s.RLock()
	defer s.RUnlock()
	cp.Classes = make([]*models.Class, len(s.Classes))
	i := 0
	for _, meta := range s.Classes {
		cp.Classes[i] = meta.CloneClass()
		i++
	}

	return cp
}

// ShardOwner returns the node owner of the specified shard
func (s *schema) ShardOwner(class, shard string) (string, uint64, error) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", 0, errClassNotFound
	}

	return meta.ShardOwner(shard)
}

// ShardFromUUID returns shard name of the provided uuid
func (s *schema) ShardFromUUID(class string, uuid []byte) (string, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", 0
	}
	return meta.ShardFromUUID(uuid)
}

// ShardReplicas returns the replica nodes of a shard
func (s *schema) ShardReplicas(class, shard string) ([]string, uint64, error) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, 0, errClassNotFound
	}
	return meta.ShardReplicas(shard)
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (s *schema) TenantsShards(class string, tenants ...string) (map[string]string, uint64) {
	s.RLock()
	defer s.RUnlock()

	meta := s.Classes[class]
	if meta == nil {
		return nil, 0
	}

	return meta.TenantsShards(class, tenants...)
}

func (s *schema) CopyShardingState(class string) (*sharding.State, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, 0
	}

	return meta.CopyShardingState()
}

func (s *schema) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	return s.shardReader.GetShardsStatus(class, tenant)
}

type shardReader interface {
	GetShardsStatus(class, tenant string) (models.ShardStatusList, error)
}

func NewSchema(nodeID string, shardReader shardReader) *schema {
	return &schema{
		nodeID:      nodeID,
		Classes:     make(map[string]*metaClass, 128),
		shardReader: shardReader,
	}
}

func (s *schema) len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.Classes)
}

func (s *schema) multiTenancyEnabled(class string) (bool, *metaClass, ClassInfo, error) {
	s.Lock()
	defer s.Unlock()
	meta := s.Classes[class]
	info := s.Classes[class].ClassInfo()
	if meta == nil {
		return false, nil, ClassInfo{}, errClassNotFound
	}
	if !info.MultiTenancy.Enabled {
		return false, nil, ClassInfo{}, fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}
	return true, meta, info, nil
}

func (s *schema) addClass(cls *models.Class, ss *sharding.State, v uint64) error {
	s.Lock()
	defer s.Unlock()
	_, exists := s.Classes[cls.Class]
	if exists {
		return errClassExists
	}

	s.Classes[cls.Class] = &metaClass{Class: *cls, Sharding: *ss, ClassVersion: v, ShardVersion: v}
	return nil
}

// updateClass modifies existing class based on the givin update function
func (s *schema) updateClass(name string, f func(*metaClass) error) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[name]
	if meta == nil {
		return errClassNotFound
	}
	return meta.LockGuard(f)
}

func (s *schema) deleteClass(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.Classes, name)
}

func (s *schema) addProperty(class string, v uint64, props ...*models.Property) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}
	return meta.AddProperty(v, props...)
}

func (s *schema) addTenants(class string, v uint64, req *command.AddTenantsRequest) error {
	req.Tenants = removeNilTenants(req.Tenants)

	if ok, meta, info, err := s.multiTenancyEnabled(class); !ok {
		return err
	} else {
		return meta.AddTenants(s.nodeID, req, int64(info.ReplicationFactor), v)
	}
}

func (s *schema) deleteTenants(class string, v uint64, req *command.DeleteTenantsRequest) error {
	if ok, meta, _, err := s.multiTenancyEnabled(class); !ok {
		return err
	} else {
		return meta.DeleteTenants(req, v)
	}
}

func (s *schema) updateTenants(class string, v uint64, req *command.UpdateTenantsRequest) (n int, err error) {
	if ok, meta, _, err := s.multiTenancyEnabled(class); !ok {
		return 0, err
	} else {
		return meta.UpdateTenants(s.nodeID, req, v)
	}
}

func (s *schema) getTenants(class string, tenants []string) ([]*models.Tenant, error) {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return nil, err
	}

	// Read tenants using the meta lock guard
	var res []*models.Tenant
	limit := 1000
	after := uuid.New().String()
	f := func(_ *models.Class, ss *sharding.State) error {
		if len(tenants) > 0 {
			measurePerf(func() { res = getTenantsByNames(ss.Physical, tenants) })
			return nil
		}
		// measurePerf(func() { res = getAllTenants_v0(ss.Physical) })
		// measurePerf(func() { res = getAllTenants_sort(ss.Physical, limit, after) })
		// measurePerf(func() { res = getAllTenants_heap(ss.Physical, limit, after) })
		measurePerf(func() { res = getAllTenants_pq(ss.Physical, limit, after) })
		return nil
	}
	return res, meta.RLockGuard(f)
}

func getAllTenants_v0(shards map[string]sharding.Physical) []*models.Tenant {
	res := make([]*models.Tenant, len(shards))
	i := 0
	for tenant := range shards {
		res[i] = makeTenant(tenant, entSchema.ActivityStatus(shards[tenant].Status))
		i++
	}
	return res
}

func getAllTenants_sort(shards map[string]sharding.Physical, limit int, after string) []*models.Tenant {
	sortedTenants := make([]string, len(shards))
	// TODO replace with append for clarity?
	i := 0
	for tenant := range shards {
		sortedTenants[i] = tenant
		i++
	}
	slices.Sort(sortedTenants)
	// TODO double check found/index one-off
	sortedIndex, found := slices.BinarySearch(sortedTenants, after)
	if found {
		sortedIndex++
	}
	numResults := len(sortedTenants) - sortedIndex
	if limit < numResults {
		numResults = limit
	}
	res := make([]*models.Tenant, numResults)
	resultIndex := 0
	for resultIndex < numResults {
		tenant := sortedTenants[sortedIndex]
		sortedIndex++
		res[resultIndex] = makeTenant(tenant, entSchema.ActivityStatus(shards[tenant].Status))
		resultIndex++
	}
	return res
}

type stringHeap []string

func (h stringHeap) Len() int           { return len(h) }
func (h stringHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h stringHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *stringHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	// TODO why doesn't this call up?
	*h = append(*h, x.(string))
}

func (h *stringHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func getAllTenants_heap(shards map[string]sharding.Physical, limit int, after string) []*models.Tenant {
	tenantsToReturn := &stringHeap{}
	for tenant := range shards {
		if tenant <= after {
			continue
		}
		heap.Push(tenantsToReturn, tenant)
		if tenantsToReturn.Len() > limit {
			heap.Pop(tenantsToReturn)
		}
	}
	res := make([]*models.Tenant, tenantsToReturn.Len())
	resIndex := 0
	// TODO only one of these checks?
	for tenantsToReturn.Len() > 0 && resIndex < len(res) {
		tenant := heap.Pop(tenantsToReturn).(string)
		// this is a min heap, so popping gives tenants in sorted order
		res[resIndex] = makeTenant(tenant, entSchema.ActivityStatus(shards[tenant].Status))
		resIndex++
	}
	return res
}

func getTenantsByNames(shards map[string]sharding.Physical, tenants []string) []*models.Tenant {
	res := make([]*models.Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		if status, ok := shards[tenant]; ok {
			res = append(res, makeTenant(tenant, entSchema.ActivityStatus(status.Status)))
		}
	}
	return res
}

// TODO copeid from vector dist queue file
type StringPriorityQueue struct {
	items []string
	less  func(items []string, i, j int) bool
}

// NewMin constructs a priority queue with smaller TODO
func NewMinStringPriorityQueue(capacity int) *StringPriorityQueue {
	return &StringPriorityQueue{
		items: make([]string, 0, capacity),
		less: func(items []string, i, j int) bool {
			return items[i] < items[j]
		},
	}
}

// Pop removes the next item in the queue and returns it
func (q *StringPriorityQueue) Pop() string {
	out := q.items[0]
	q.items[0] = q.items[len(q.items)-1]
	q.items = q.items[:len(q.items)-1]
	q.heapify(0)
	return out
}

// Top peeks at the next item in the queue
func (q *StringPriorityQueue) Top() string {
	return q.items[0]
}

// Len returns the length of the queue
func (q *StringPriorityQueue) Len() int {
	return len(q.items)
}

// Cap returns the remaining capacity of the queue
func (q *StringPriorityQueue) Cap() int {
	return cap(q.items)
}

// Reset clears all items from the queue
func (q *StringPriorityQueue) Reset() {
	q.items = q.items[:0]
}

// ResetCap drops existing queue items, and allocates a new queue with the given capacity
func (q *StringPriorityQueue) ResetCap(capacity int) {
	q.items = make([]string, 0, capacity)
}

// Insert creates a valueless item and adds it to the queue
func (q *StringPriorityQueue) Insert(item string) int {
	return q.insert(item)
}

// InsertWithValue creates an item with a T type value and adds it to the queue
func (q *StringPriorityQueue) InsertWithValue(item string) int {
	return q.insert(item)
}

func (q *StringPriorityQueue) insert(item string) int {
	q.items = append(q.items, item)
	i := len(q.items) - 1
	for i != 0 && q.less(q.items, i, q.parent(i)) {
		q.swap(i, q.parent(i))
		i = q.parent(i)
	}
	return i
}

func (q *StringPriorityQueue) left(i int) int {
	return 2*i + 1
}

func (q *StringPriorityQueue) right(i int) int {
	return 2*i + 2
}

func (q *StringPriorityQueue) parent(i int) int {
	return (i - 1) / 2
}

func (q *StringPriorityQueue) swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *StringPriorityQueue) heapify(i int) {
	left := q.left(i)
	right := q.right(i)
	smallest := i
	if left < len(q.items) && q.less(q.items, left, i) {
		smallest = left
	}

	if right < len(q.items) && q.less(q.items, right, smallest) {
		smallest = right
	}

	if smallest != i {
		q.swap(i, smallest)
		q.heapify(smallest)
	}
}

func getAllTenants_pq(shards map[string]sharding.Physical, limit int, after string) []*models.Tenant {
	pq := NewMinStringPriorityQueue(limit)
	for tenant := range shards {
		// ignore all tenants which come before the "after" arg
		if tenant <= after {
			continue
		}
		// go through the rest of the tenants storing only the ones we're going to return
		pq.Insert(tenant)
		if pq.Len() > limit {
			pq.Pop()
		}
	}
	res := make([]*models.Tenant, pq.Len())
	resIndex := 0
	// TODO only one of these checks?
	for pq.Len() > 0 && resIndex < len(res) {
		// this is a min heap, so popping gives tenants in sorted order
		tenant := pq.Pop()
		res[resIndex] = makeTenant(tenant, entSchema.ActivityStatus(shards[tenant].Status))
		resIndex++
	}
	return res
}

func measurePerf(f func()) {
	timeBefore := time.Now()
	memBefore := runtime.MemStats{}
	runtime.ReadMemStats(&memBefore)
	f()
	memAfter := runtime.MemStats{}
	runtime.ReadMemStats(&memAfter)
	timeAfter := time.Now()
	memAllocDiff := memAfter.Alloc - memBefore.Alloc
	memTotalAllocDiff := memAfter.TotalAlloc - memBefore.TotalAlloc
	timeDiffNano := timeAfter.UnixNano() - timeBefore.UnixNano()
	fmt.Println("MEASUREPERF:MEMALLOC:", memAfter.Alloc)
	fmt.Println("MEASUREPERF:MEMTOTALALLOC:", memAfter.TotalAlloc)
	fmt.Println("MEASUREPERF:MEMALLOCDIFF:", memAllocDiff)
	fmt.Println("MEASUREPERF:MEMTOTALALLOCDIFF:", memTotalAllocDiff)
	fmt.Println("MEASUREPERF:TIMEDIFFNS:", timeDiffNano)
}

func (s *schema) States() map[string]ClassState {
	s.RLock()
	defer s.RUnlock()

	cs := make(map[string]ClassState, len(s.Classes))
	for _, c := range s.Classes {
		cs[c.Class.Class] = ClassState{
			Class:  c.Class,
			Shards: c.Sharding,
		}
	}

	return cs
}

func (s *schema) clear() {
	s.Lock()
	defer s.Unlock()
	for k := range s.Classes {
		delete(s.Classes, k)
	}
}

func makeTenant(name, status string) *models.Tenant {
	return &models.Tenant{
		Name:           name,
		ActivityStatus: status,
	}
}
