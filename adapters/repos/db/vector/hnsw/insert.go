//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func (h *hnsw) ValidateBeforeInsert(vector []float32) error {
	if h.isEmpty() {
		return nil
	}
	// check if vector length is the same as existing nodes
	existingNodeVector, err := h.cache.get(context.Background(), h.entryPointID)
	if err != nil {
		return err
	}

	if len(existingNodeVector) != len(vector) {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), len(existingNodeVector))
	}

	return nil
}

func (h *hnsw) Add(id uint64, vector []float32) error {
	before := time.Now()
	if len(vector) == 0 {
		return errors.Errorf("insert called with nil-vector")
	}

	h.metrics.InsertVector()
	defer h.insertMetrics.total(before)

	node := &vertex{
		id: id,
	}

	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}

	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()
	return h.insert(node, vector)
}

func (h *hnsw) insertInitialElement(node *vertex, nodeVec []float32) error {
	h.Lock()
	defer h.Unlock()

	if err := h.commitLog.SetEntryPointWithMaxLayer(node.id, 0); err != nil {
		return err
	}

	h.entryPointID = node.id
	h.currentMaximumLayer = 0
	node.connections = [][]uint64{
		make([]uint64, 0, h.maximumConnectionsLayerZero),
	}
	node.level = 0
	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}

	h.nodes[node.id] = node
	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.preload(node.id, compressed)
	} else {
		h.cache.preload(node.id, nodeVec)
	}

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) insert(node *vertex, nodeVec []float32) error {
	h.deleteVsInsertLock.RLock()
	defer h.deleteVsInsertLock.RUnlock()

	before := time.Now()

	wasFirst := false
	var firstInsertError error
	h.initialInsertOnce.Do(func() {
		if h.isEmpty() {
			wasFirst = true
			firstInsertError = h.insertInitialElement(node, nodeVec)
		}
	})
	if wasFirst {
		return firstInsertError
	}

	node.markAsMaintenance()

	h.RLock()
	// initially use the "global" entrypoint which is guaranteed to be on the
	// currently highest layer
	entryPointID := h.entryPointID
	// initially use the level of the entrypoint which is the highest level of
	// the h-graph in the first iteration
	currentMaximumLayer := h.currentMaximumLayer
	h.RUnlock()

	targetLevel := int(math.Floor(-math.Log(h.randFunc()) * h.levelNormalizer))

	// before = time.Now()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
	node.connections = make([][]uint64, targetLevel+1)

	for i := targetLevel; i >= 0; i-- {
		capacity := h.maximumConnections
		if i == 0 {
			capacity = h.maximumConnectionsLayerZero
		}

		node.connections[i] = make([]uint64, 0, capacity)
	}

	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	nodeId := node.id

	h.Lock()
	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}
	h.nodes[nodeId] = node
	h.Unlock()

	// make sure this new vec is immediately present in the cache, so we don't
	// have to read it from disk again
	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.preload(node.id, compressed)
	} else {
		h.cache.preload(node.id, nodeVec)
	}

	h.insertMetrics.prepareAndInsertNode(before)
	before = time.Now()

	entryPointID, err = h.findBestEntrypointForNode(currentMaximumLayer, targetLevel,
		entryPointID, nodeVec)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	h.insertMetrics.findEntrypoint(before)
	before = time.Now()

	if err := h.findAndConnectNeighbors(node, entryPointID, nodeVec,
		targetLevel, currentMaximumLayer, helpers.NewAllowList()); err != nil {
		return errors.Wrap(err, "find and connect neighbors")
	}

	h.insertMetrics.findAndConnectTotal(before)
	before = time.Now()
	defer h.insertMetrics.updateGlobalEntrypoint(before)

	// go h.insertHook(nodeId, targetLevel, neighborsAtLevel)
	node.unmarkAsMaintenance()

	h.RLock()
	if targetLevel > h.currentMaximumLayer {
		h.RUnlock()
		h.Lock()
		//check again to avoid changes from RUnlock to Lock again
		if targetLevel > h.currentMaximumLayer {
			// before = time.Now()
			// m.addBuildingLocking(before)
			if err := h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel); err != nil {
				h.Unlock()
				return err
			}

			h.entryPointID = nodeId
			h.currentMaximumLayer = targetLevel
		}
		h.Unlock()
	} else {
		h.RUnlock()
	}

	return nil
}
