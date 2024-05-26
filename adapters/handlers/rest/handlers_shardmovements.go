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

package rest

import (
	"fmt"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/shardmovements"
	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/models"
)

type ShardMovement struct {
	ClusterService *cluster.Service
}

func (sm ShardMovement) addShardMovement(params shardmovements.ShardmovementsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	fmt.Println("NATEE addShardMovement", params.Body, sm.ClusterService.Store.DB.Schema.Classes["Foo"].Sharding.Physical[params.Body.ShardName].BelongsToNodes)
	p := sm.ClusterService.Store.DB.Schema.Classes["Foo"].Sharding.Physical[params.Body.ShardName]
	p.BelongsToNodes = []string{"weaviate-2"}
	sm.ClusterService.Store.DB.Schema.Classes["Foo"].Sharding.Physical[params.Body.ShardName] = p
	return shardmovements.NewShardmovementsCreateOK().WithPayload(params.Body)
}
