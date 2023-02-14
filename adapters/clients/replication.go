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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

// ReplicationClient is to coordinate operations among replicas

type replicationClient retryClient

func NewReplicationClient(httpClient *http.Client) replica.Client {
	return &replicationClient{
		client:  httpClient,
		retryer: newRetryer(),
	}
}

// FetchObject fetches one object it exits
func (c *replicationClient) FetchObject(ctx context.Context, host, index,
	shard string, id strfmt.UUID, selectProps search.SelectProperties,
	additional additional.Properties,
) (objects.Replica, error) {
	resp := objects.Replica{}
	req, err := newHttpReplicaRequest(ctx, http.MethodGet, host, index, shard, "", id.String(), nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}
	// TODO: fix c.do, or dont use it
	err = c.do(c.timeoutUnit*90, req, nil, resp)
	return resp, err
}

// Exists
func (c *replicationClient) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (objects.Replica, error) {
	return objects.Replica{}, fmt.Errorf("not implemented")
}

// FetchObjects fetches objects specified in ids
func (c *replicationClient) FetchObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]objects.Replica, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *replicationClient) DigestObjects(ctx context.Context,
	host, index, shard string, ids []strfmt.UUID,
) (result []replica.RepairResponse, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *replicationClient) OverwriteObjects(ctx context.Context,
	host, index, shard string, objects []*objects.VObject,
) ([]replica.RepairResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *replicationClient) FindObject(_ context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (objects.Replica, error) {
	return objects.Replica{}, nil
}

func (c *replicationClient) MultiGetObjects(_ context.Context, host,
	index, shard string, ids []strfmt.UUID,
) ([]objects.Replica, error) {
	return nil, nil
}

func (c *replicationClient) PutObject(ctx context.Context, host, index,
	shard, requestID string, obj *storobj.Object,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.SingleObject.Marshal(obj)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpReplicaRequest(ctx, http.MethodPost, host, index, shard, requestID, "", nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.SingleObject.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp)
	return resp, err
}

func (c *replicationClient) DeleteObject(ctx context.Context, host, index,
	shard, requestID string, uuid strfmt.UUID,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	req, err := newHttpReplicaRequest(ctx, http.MethodDelete, host, index, shard, requestID, uuid.String(), nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	err = c.do(c.timeoutUnit*90, req, nil, &resp)
	return resp, err
}

func (c *replicationClient) PutObjects(ctx context.Context, host, index,
	shard, requestID string, objects []*storobj.Object,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objects)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(ctx, http.MethodPost, host, index, shard, requestID, "", nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ObjectList.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp)
	return resp, err
}

func (c *replicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.MergeDoc.Marshal(*doc)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpReplicaRequest(ctx, http.MethodPatch, host, index, shard,
		requestID, doc.ID.String(), nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.MergeDoc.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp)
	return resp, err
}

func (c *replicationClient) AddReferences(ctx context.Context, host, index,
	shard, requestID string, refs []objects.BatchReference,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.ReferenceList.Marshal(refs)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(ctx, http.MethodPost, host, index, shard,
		requestID, "references", nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ReferenceList.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp)
	return resp, err
}

func (c *replicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	docIDs []uint64, dryRun bool,
) (resp replica.SimpleResponse, err error) {
	body, err := clusterapi.IndicesPayloads.BatchDeleteParams.Marshal(docIDs, dryRun)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(ctx, http.MethodDelete, host, index, shard, requestID, "", nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp)
	return resp, err
}

// Commit asks a host to commit and stores the response in the value pointed to by resp
func (c *replicationClient) Commit(ctx context.Context, host, index, shard string, requestID string, resp interface{}) error {
	req, err := newHttpReplicaCMD(host, "commit", index, shard, requestID, nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	return c.do(c.timeoutUnit*90, req, nil, resp)
}

func (c *replicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (
	resp replica.SimpleResponse, err error,
) {
	req, err := newHttpReplicaCMD(host, "abort", index, shard, requestID, nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	err = c.do(c.timeoutUnit*5, req, nil, &resp)
	return resp, err
}

func newHttpReplicaRequest(ctx context.Context, method, host, index, shard, requestId, suffix string, body io.Reader) (*http.Request, error) {
	path := fmt.Sprintf("/replicas/indices/%s/shards/%s/objects", index, shard)
	if suffix != "" {
		path = fmt.Sprintf("%s/%s", path, suffix)
	}
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path,
	}

	if requestId != "" {
		u.RawQuery = url.Values{replica.RequestKey: []string{requestId}}.Encode()
	}

	return http.NewRequestWithContext(ctx, method, u.String(), body)
}

func newHttpReplicaCMD(host, cmd, index, shard, requestId string, body io.Reader) (*http.Request, error) {
	path := fmt.Sprintf("/replicas/indices/%s/shards/%s:%s", index, shard, cmd)
	q := url.Values{replica.RequestKey: []string{requestId}}.Encode()
	url := url.URL{Scheme: "http", Host: host, Path: path, RawQuery: q}
	return http.NewRequest(http.MethodPost, url.String(), body)
}

func (c *replicationClient) do(timeout time.Duration, req *http.Request, body []byte, resp interface{}) (err error) {
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	try := func(ctx context.Context) (bool, error) {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			return shouldRetry(code), fmt.Errorf("status code: %v", code)
		}
		if err := json.NewDecoder(res.Body).Decode(resp); err != nil {
			return false, fmt.Errorf("decode response: %w", err)
		}
		return false, nil
	}
	return c.retry(ctx, 9, try)
}

// backOff return a new random duration in the interval [d, 3d].
// It implements truncated exponential back-off with introduced jitter.
func backOff(d time.Duration) time.Duration {
	return time.Duration(float64(d.Nanoseconds()*2) * (0.5 + rand.Float64()))
}

func shouldRetry(code int) bool {
	return code == http.StatusInternalServerError ||
		code == http.StatusTooManyRequests ||
		code == http.StatusServiceUnavailable
}
