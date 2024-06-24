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
	"strings"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type shardCycleCallbacks struct {
	compactionCallbacks     cyclemanager.CycleCallbackGroup
	compactionCallbacksCtrl cyclemanager.CycleCallbackCtrl

	flushCallbacks     cyclemanager.CycleCallbackGroup
	flushCallbacksCtrl cyclemanager.CycleCallbackCtrl

	vectorCommitLoggerCallbacks     cyclemanager.CycleCallbackGroup
	vectorTombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup
	vectorCombinedCallbacksCtrl     cyclemanager.CycleCallbackCtrl

	geoPropsCommitLoggerCallbacks     cyclemanager.CycleCallbackGroup
	geoPropsTombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup
	geoPropsCombinedCallbacksCtrl     cyclemanager.CycleCallbackCtrl
}

func (s *Shard) initCycleCallbacks() {
	id := func(elems ...string) string {
		elems = append([]string{"shard", s.index.ID(), s.name}, elems...)
		return strings.Join(elems, "/")
	}

	compactionId := id("compaction")
	compactionCallbacks := cyclemanager.NewCallbackGroup(compactionId, s.index.Logger, 1)
	compactionCallbacksCtrl := s.index.CycleCallbacks.compactionCallbacks.Register(
		compactionId, compactionCallbacks.CycleCallback,
		cyclemanager.WithIntervals(cyclemanager.CompactionCycleIntervals()))

	flushId := id("flush")
	flushCallbacks := cyclemanager.NewCallbackGroup(flushId, s.index.Logger, 1)
	flushCallbacksCtrl := s.index.CycleCallbacks.flushCallbacks.Register(
		flushId, flushCallbacks.CycleCallback,
		cyclemanager.WithIntervals(cyclemanager.MemtableFlushCycleIntervals()))

	vectorCommitLoggerId := id("vector", "commit_logger")
	vectorCommitLoggerCallbacks := cyclemanager.NewCallbackGroup(vectorCommitLoggerId, s.index.Logger, 1)
	vectorCommitLoggerCallbacksCtrl := s.index.CycleCallbacks.vectorCommitLoggerCallbacks.Register(
		vectorCommitLoggerId, vectorCommitLoggerCallbacks.CycleCallback,
		cyclemanager.WithIntervals(cyclemanager.HnswCommitLoggerCycleIntervals()))

	vectorTombstoneCleanupId := id("vector", "tombstone_cleanup")
	vectorTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup(vectorTombstoneCleanupId, s.index.Logger, 1)
	// fixed interval on class level, no need to specify separate on shard level
	vectorTombstoneCleanupCallbacksCtrl := s.index.CycleCallbacks.vectorTombstoneCleanupCallbacks.Register(
		vectorTombstoneCleanupId, vectorTombstoneCleanupCallbacks.CycleCallback)

	vectorCombinedCallbacksCtrl := cyclemanager.NewCombinedCallbackCtrl(2, s.index.Logger,
		vectorCommitLoggerCallbacksCtrl, vectorTombstoneCleanupCallbacksCtrl)

	geoPropsCommitLoggerId := id("geo_props", "commit_logger")
	geoPropsCommitLoggerCallbacks := cyclemanager.NewCallbackGroup(geoPropsCommitLoggerId, s.index.Logger, 1)
	geoPropsCommitLoggerCallbacksCtrl := s.index.CycleCallbacks.geoPropsCommitLoggerCallbacks.Register(
		geoPropsCommitLoggerId, geoPropsCommitLoggerCallbacks.CycleCallback,
		cyclemanager.WithIntervals(cyclemanager.GeoCommitLoggerCycleIntervals()))

	geoPropsTombstoneCleanupId := id("geoProps", "tombstone_cleanup")
	geoPropsTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup(geoPropsTombstoneCleanupId, s.index.Logger, 1)
	// fixed interval on class level, no need to specify separate on shard level
	geoPropsTombstoneCleanupCallbacksCtrl := s.index.CycleCallbacks.geoPropsTombstoneCleanupCallbacks.Register(
		geoPropsTombstoneCleanupId, geoPropsTombstoneCleanupCallbacks.CycleCallback)

	geoPropsCombinedCallbacksCtrl := cyclemanager.NewCombinedCallbackCtrl(2, s.index.Logger,
		geoPropsCommitLoggerCallbacksCtrl, geoPropsTombstoneCleanupCallbacksCtrl)

	s.cycleCallbacks = &shardCycleCallbacks{
		compactionCallbacks:     compactionCallbacks,
		compactionCallbacksCtrl: compactionCallbacksCtrl,

		flushCallbacks:     flushCallbacks,
		flushCallbacksCtrl: flushCallbacksCtrl,

		vectorCommitLoggerCallbacks:     vectorCommitLoggerCallbacks,
		vectorTombstoneCleanupCallbacks: vectorTombstoneCleanupCallbacks,
		vectorCombinedCallbacksCtrl:     vectorCombinedCallbacksCtrl,

		geoPropsCommitLoggerCallbacks:     geoPropsCommitLoggerCallbacks,
		geoPropsTombstoneCleanupCallbacks: geoPropsTombstoneCleanupCallbacks,
		geoPropsCombinedCallbacksCtrl:     geoPropsCombinedCallbacksCtrl,
	}
}
