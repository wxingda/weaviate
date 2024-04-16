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

package objects

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

var errEmptyObjects = NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least one object for batching")

type ClassToObjects struct {
	class   *models.Class
	objects []*BatchObject
}

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	before := time.Now()
	b.metrics.BatchInc()
	defer func() {
		b.metrics.BatchDec()
		b.metrics.BatchOp("total_uc_level", before.UnixNano())
	}()

	beforePreProcessing := time.Now()
	if len(objects) == 0 {
		return nil, errEmptyObjects
	}

	ctos := map[string]*ClassToObjects{}
	for idx, object := range objects {
		bob := &BatchObject{Object: object, OriginalIndex: idx}

		if len(object.Class) == 0 {
			bob.Err = fmt.Errorf(validation.ErrorMissingClass)
			continue
		}
		object.Class = schema.UppercaseClassName(object.Class)
		existedClasses, errs := b.autoSchemaManager.autoSchema(ctx, principal, true, object)
		if len(errs) > 0 && errs[0] != nil {
			bob.Err = err
			continue
		}
		existedCto, ok := ctos[object.Class]
		if !ok {
			cls, _, err := b.schemaManager.GetClass(ctx, principal, object.Class)
			if err != nil {
				bob.Err = err
				continue
			}

			if cls == nil {
				bob.Err = fmt.Errorf("class '%v' not present in schema", object.Class)
				continue
			}

			ctos[object.Class] = &ClassToObjects{
				class:   cls,
				objects: []*BatchObject{bob},
			}
			continue
		}
		existedCto.objects = append(existedCto.objects, bob)
		ctos[object.Class] = existedCto
	}

	b.planAddObjects(ctx, ctos, repl)

	if err := b.autoSchemaManager.autoTenants(ctx, principal, objects); err != nil {
		return nil, fmt.Errorf("auto create tenants: %w", err)
	}

	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	var res BatchObjects
	for _, cto := range ctos {
		for _, obj := range cto.objects {
			res = append(res, *obj)
		}
	}
	for _, x := range res {
		x.Err = nil
	}
	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())
	if res, err = b.vectorRepo.BatchPutObjects(ctx, res, repl); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) planAddObjects(ctx context.Context,
	ctos map[string]*ClassToObjects, repl *additional.ReplicationProperties,
) {
	var (
		now       = time.Now().UnixNano() / int64(time.Millisecond)
		validator = validation.New(b.vectorRepo.Exists, b.config, repl)
	)

	// validate each object and sort by class (==vectorizer)
	for _, cto := range ctos {
		for _, obj := range cto.objects {
			if obj.Err != nil {
				continue
			}

			// Generate UUID for the new object
			if obj.Object.ID == "" {
				uid, err := generateUUID()
				obj.Object.ID = uid
				obj.Err = err
			} else {
				if _, err := uuid.Parse(obj.Object.ID.String()); err != nil {
					obj.Err = err
					continue
				}
			}

			if obj.Object.Properties == nil {
				obj.Object.Properties = map[string]interface{}{}
			}

			obj.Object.CreationTimeUnix = now
			obj.Object.LastUpdateTimeUnix = now
			obj.UUID = obj.Object.ID
			if obj.Err != nil {
				continue
			}

			if err := validator.Object(ctx, cto.class, obj.Object, nil); err != nil {
				obj.Err = err
				continue
			}

			errorsPerObj, err := b.modulesProvider.BatchUpdateVector(ctx, cto.class, []*models.Object{obj.Object}, b.findObject, b.logger)
			if err != nil {
				obj.Err = err
			}

			for _, err := range errorsPerObj {
				obj.Err = err
			}
		}
	}
}
