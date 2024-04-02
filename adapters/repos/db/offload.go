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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/offload"
	"github.com/weaviate/weaviate/entities/schema"
)

// OffloadDescriptors returns a channel of shard descriptors.
// Shard descriptor records everything needed to restore a tenant.
func (db *DB) OffloadDescriptors(ctx context.Context, class string, tenants []string,
) <-chan offload.ShardDescriptor {
	l := len(tenants)
	ch := make(chan offload.ShardDescriptor, l)

	if l == 0 {
		close(ch)
		return ch
	}

	go func() {
		var errIdx error
		idx := db.GetIndex(schema.ClassName(class))
		if idx == nil {
			errIdx = fmt.Errorf("class %v doesn't exist any more", class)
		}

		for _, tenant := range tenants {
			desc := offload.ShardDescriptor{}
			err := func() error {
				if errIdx != nil {
					return errIdx
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				shard := idx.shards.Load(tenant)
				if shard == nil {
					return fmt.Errorf("tenant %v doesn't exist any more", tenant)
				}
				return shard.offloadDescriptor(ctx, &desc)
			}()

			desc.Class = class
			desc.Tenant = tenant
			desc.Error = err
			ch <- desc
		}
		close(ch)
	}()

	return ch
}

func (db *DB) ReleaseOffload(ctx context.Context, class, tenant string, successfulOffload bool) (err error) {
	fields := logrus.Fields{
		"op":     "release_offload",
		"class":  class,
		"tenant": tenant,
	}
	db.logger.WithFields(fields).Debug("starting")
	start := time.Now()
	defer func() {
		l := db.logger.WithFields(fields).WithField("took", time.Since(start))
		if err != nil {
			l.Error(err)
			return
		}
		l.Debug("finished")
	}()

	idx := db.GetIndex(schema.ClassName(class))
	if idx == nil {
		err = fmt.Errorf("release offload: class %v doesn't exist any more", class)
		return
	}
	shard := idx.shards.Load(tenant)
	if shard == nil {
		err = fmt.Errorf("release offload: tenant %v doesn't exist any more", tenant)
		return
	}

	if successfulOffload {
		err = shard.releaseSuccessfulOffload(ctx)
	} else {
		err = shard.releaseFailedOffload(ctx)
	}
	return
}
