package db

import (
	"context"

	"github.com/pkg/errors"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func (s *Shard) Reindex(ctx context.Context) error {
	if !asyncEnabled() {
		return errors.New("async indexed is not enabled")
	}

	_, ok := s.vectorIndexes["reindex.main"]
	if ok {
		return errors.New("re-indexing is already running")
	}

	targetVector := "reindex.main"

	var err error
	if s.vectorIndexes == nil {
		s.vectorIndexes = make(map[string]VectorIndex)
	}

	s.vectorIndexes[targetVector], err = s.initVectorIndex(ctx, targetVector, s.index.vectorIndexUserConfig)
	if err != nil {
		return errors.Wrap(err, "init vector index")
	}

	if s.queues == nil {
		s.queues = make(map[string]*IndexQueue)
	}

	q, err := NewIndexQueue(
		s.index.Config.ClassName.String(),
		s.ID(),
		targetVector,
		s,
		s.vectorIndexes[targetVector],
		s.centralJobQueue,
		s.indexCheckpoints,
		IndexQueueOptions{Logger: s.index.logger},
		s.promMetrics,
	)
	if err != nil {
		return errors.Wrap(err, "init index queue")
	}
	s.queues[targetVector] = q

	enterrors.GoWrapper(func() {
		count, err := s.queues[targetVector].loadFromDisk(s, "", 0)
		if err != nil {
			s.index.logger.WithError(err).Error("preload shard")
		}

		s.index.logger.WithField("count", count).Info("preload shard")
	}, s.index.logger)

	return nil
}
