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

package hnsw

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/storobj"
	"golang.org/x/exp/slices"
)

const MaxFilters = 2

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue[any],
	max int, denyList helpers.AllowList, labels []byte,
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
		bag := h.compressor.NewBag()
		for _, id := range ids {
			err := bag.Load(context.Background(), id)
			if err != nil {
				return err
			}
		}

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.Item[uint64])
		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			good := true
			for _, item := range returnList {
				peerDist, err := bag.Distance(curr.ID, item.ID)
				if err != nil {
					return err
				}

				if peerDist < distToQuery && containsAll(h.nodes[item.ID].labels, h.nodes[curr.ID].labels) {
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
		sums := make([]int, MaxFilters)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			currLabels := h.nodes[curr.ID].labels
			allAreSaturated := true
			for _, sumIndex := range currLabels {
				if sums[sumIndex] < max/MaxFilters {
					allAreSaturated = false
					break
				}
			}
			if allAreSaturated {
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

				if peerDist < distToQuery && containsAll(h.nodes[item.ID].labels, h.nodes[curr.ID].labels) {
					good = false
					break
				}
			}

			if good {
				for sumIndex := range h.nodes[curr.ID].labels {
					sums[sumIndex]++
				}
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

func containsAll(all, part []byte) bool {
	for _, x := range part {
		if !slices.Contains(all, x) {
			return false
		}
	}
	return true
}
