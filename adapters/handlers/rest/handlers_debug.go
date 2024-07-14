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
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/configbase"
)

func setupDebugHandlers(appState *state.State) {
	logger := appState.Logger

	http.HandleFunc("/debug/reindex/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !configbase.Enabled(os.Getenv("ASYNC_INDEXING")) {
			http.Error(w, "async indexing is not enabled", http.StatusNotImplemented)
			return
		}

		path := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/debug/reindex/collection/"))
		parts := strings.Split(path, "/")
		if len(parts) < 3 || len(parts) > 5 || parts[1] != "shards" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		colName, shardName := parts[0], parts[2]
		vecIdxID := "main"
		if len(parts) == 4 {
			vecIdxID = parts[3]
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found")
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		shard := idx.GetShard(shardName)
		if shard == nil {
			logger.WithField("shard", shardName).Error("shard not found")
			http.Error(w, "shard not found", http.StatusNotFound)
			return
		}

		err := shard.Reindex(context.Background())
		if err != nil {
			logger.WithError(err).Error("reindexing failed")
			http.Error(w, "reindexing failed", http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "reindexing started for collection %s, shard %s, vector index %s", colName, shardName, vecIdxID)

		w.WriteHeader(http.StatusAccepted)
	}))
}
