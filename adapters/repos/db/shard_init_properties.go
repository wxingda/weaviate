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
	"context"
	"fmt"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/models"
)

func (s *Shard) initProperties(class *models.Class) error {
	fmt.Printf("  ==> [%s][%s] initProperties: start\n", s.name, time.Now())
	defer func() {
		fmt.Printf("  ==> [%s][%s] initProperties: end\n", s.name, time.Now())
	}()

	s.propertyIndices = propertyspecific.Indices{}
	if class == nil {
		return nil
	}

	eg := enterrors.NewErrorGroupWrapper(s.index.logger)
	s.createPropertyIndex(context.Background(), eg, class.Properties...)

	fmt.Printf("  ==> [%s][%s] initProperties: createPropertyIndex\n", s.name, time.Now())

	eg.Go(func() error {
		if err := s.addIDProperty(context.TODO()); err != nil {
			return errors.Wrap(err, "create id property index")
		}
		fmt.Printf("  ==> [%s][%s] initProperties: addIDProperty\n", s.name, time.Now())
		return nil
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			if err := s.addTimestampProperties(context.TODO()); err != nil {
				return errors.Wrap(err, "create timestamp properties indexes")
			}
			fmt.Printf("  ==> [%s][%s] initProperties: addTimestampProperties\n", s.name, time.Now())
			return nil
		})
	}

	if s.index.Config.TrackVectorDimensions {
		eg.Go(func() error {
			if err := s.addDimensionsProperty(context.TODO()); err != nil {
				return errors.Wrap(err, "crreate dimensions property index")
			}
			fmt.Printf("  ==> [%s][%s] initProperties: addDimensionsProperty\n", s.name, time.Now())
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "init properties on shard '%s'", s.ID())
	}
	return nil
}
