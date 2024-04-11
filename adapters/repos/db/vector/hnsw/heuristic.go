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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue[any],
	max int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.InsertWithValue(elem.ID, elem.Dist, i)
		ids[i] = elem.ID
		i++
	}

	var returnList []priorityqueue.Item[uint64]

	if h.compressed.Load() {
		vecs := make([][]byte, 0, len(ids))
		for _, id := range ids {
			v, err := h.compressedVectorsCache.Get(context.Background(), id)
			if err != nil {
				return err
			}
			vecs = append(vecs, v)
		}

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.Item[uint64])

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Value]
			good := true
			for _, item := range returnList {
				peerDist := h.pq.DistanceBetweenCompressedVectors(currVec, vecs[item.Value])

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	} else {

		vecs, errs := h.multiVectorForID(context.TODO(), ids)

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.Item[uint64])

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Value]
			if err := errs[curr.Value]; err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID)
					continue
				} else {
					// not a typed error, we can recover from, return with err
					return errors.Wrapf(err,
						"unrecoverable error for docID %d", curr.ID)
				}
			}
			good := true
			for _, item := range returnList {
				peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
					vecs[item.Value])

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// rewind and return to pool
	returnList = returnList[:0]

	//nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}

func (h *hnsw) ACORNprune(node *vertex, input *priorityqueue.Queue[any], max int, denyList helpers.AllowList) error {
	// Early exit if the input size is within the allowed limit
	if input.Len() <= h.acornMBeta {
		return nil
	}

	// Use a temporary priority queue to store and prioritize neighbors for processing
	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	defer h.pools.pqHeuristic.Put(closestFirst)

	// Map to keep track of two-hop neighbors
	twoHopNeighborhood := make(map[uint64]bool)
	for _, connectedID := range node.connections[0] {
		twoHopNeighborhood[connectedID] = true
	}

	// Transfer elements to the closestFirst queue with consideration for the deny list
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		if denyList != nil && denyList.Contains(elem.ID) {
			continue
		}
		closestFirst.InsertWithValue(elem.ID, elem.Dist, i)
		i++
	}

	// Process neighbors, reinserting up to 'max' neighbors back into the input queue
	returnList := h.pools.pqItemSlice.Get().([]priorityqueue.Item[uint64])
	defer h.pools.pqItemSlice.Put(returnList[:0])

	for closestFirst.Len() > 0 && len(returnList) < max {
		curr := closestFirst.Pop()

		// Check for direct or two-hop connectivity
		if _, exists := twoHopNeighborhood[curr.ID]; !exists {
			// If not directly connected or a two-hop neighbor, evaluate further
			// This is simplified; actual logic might involve more checks or conditions

			neighbor := h.nodeByID(curr.ID)
			if neighbor != nil {
				for _, connectedID := range neighbor.connections[0] {
					twoHopNeighborhood[connectedID] = true
				}
			}

			// Append to the returnList
			returnList = append(returnList, curr)
		}
	}

	// Transfer the pruned neighbors from the returnList back to the input queue
	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	return nil
}
