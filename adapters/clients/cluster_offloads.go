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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/weaviate/weaviate/usecases/offload"
)

const (
	pathOffloadsCanCommit = "/offloads/can-commit"
	pathOffloadsCommit    = "/offloads/commit"
	pathOffloadsStatus    = "/offloads/status"
	pathOffloadsAbort     = "/offloads/abort"
)

type ClusterOffloads struct {
	client *http.Client
}

func NewClusterOffloads(client *http.Client) *ClusterOffloads {
	return &ClusterOffloads{client: client}
}

func (c *ClusterOffloads) CanCommit(ctx context.Context,
	host string, req *offload.Request,
) (*offload.CanCommitResponse, error) {
	url := url.URL{Scheme: "http", Host: host, Path: pathOffloadsCanCommit}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal can-commit request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("new can-commit request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("can-commit request: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	var resp offload.CanCommitResponse
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		return nil, fmt.Errorf("unmarshal can-commit response: %w", err)
	}

	return &resp, nil
}

func (c *ClusterOffloads) Commit(ctx context.Context,
	host string, req *offload.StatusRequest,
) error {
	url := url.URL{Scheme: "http", Host: host, Path: pathOffloadsCommit}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal commit request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new commit request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("commit request: %w", err)
	}

	if statusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	return nil
}

func (c *ClusterOffloads) Status(ctx context.Context,
	host string, req *offload.StatusRequest,
) (*offload.StatusResponse, error) {
	url := url.URL{Scheme: "http", Host: host, Path: pathOffloadsStatus}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal status request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("new status request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("status request: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	var resp offload.StatusResponse
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		return nil, fmt.Errorf("unmarshal status response: %w", err)
	}

	return &resp, nil
}

func (c *ClusterOffloads) Abort(_ context.Context,
	host string, req *offload.AbortRequest,
) error {
	url := url.URL{Scheme: "http", Host: host, Path: pathOffloadsAbort}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal abort request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new abort request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("abort request: %w", err)
	}

	if statusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	return nil
}

func (c *ClusterOffloads) do(req *http.Request) (body []byte, statusCode int, err error) {
	httpResp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("make request: %w", err)
	}

	body, err = io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, httpResp.StatusCode, fmt.Errorf("read response: %w", err)
	}
	defer httpResp.Body.Close()

	return body, httpResp.StatusCode, nil
}
