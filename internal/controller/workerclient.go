package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

var (
	// ErrWorker means the worker was unreachable or answered unexpectedly.
	ErrWorker = errors.New("worker request failed")
	// ErrChunkMissing means the worker doesn't have the chunk (404).
	ErrChunkMissing = fmt.Errorf("%w: chunk missing", ErrWorker)
	// ErrChunkRejected means the worker refused the chunk (400/409).
	ErrChunkRejected = fmt.Errorf("%w: chunk rejected", ErrWorker)
)

// WorkerClient is the controller -> worker HTTP client (pooled connections).
type WorkerClient struct {
	http    *http.Client
	timeout time.Duration
}

func NewWorkerClient(timeout time.Duration) *WorkerClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 64
	tr.MaxIdleConnsPerHost = 16
	return &WorkerClient{http: &http.Client{Transport: tr}, timeout: timeout}
}

func (c *WorkerClient) do(ctx context.Context, timeout time.Duration, method, url string, body []byte, hdr map[string]string) (*http.Response, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("%w: %s %s: %v", ErrWorker, method, url, err)
	}
	return resp, cancel, nil
}

// PutChunk stores a chunk; the worker re-verifies the checksum before committing.
func (c *WorkerClient) PutChunk(ctx context.Context, base, chunkID string, data []byte, sum, objectID string, index int) error {
	if sum == "" {
		sum = checksum.SHA256Hex(data)
	}
	hdr := map[string]string{"X-Chunk-Checksum": sum, "Content-Type": "application/octet-stream",
		"X-Chunk-Index": strconv.Itoa(index)}
	if objectID != "" {
		hdr["X-Object-Id"] = objectID
	}
	resp, cancel, err := c.do(ctx, c.timeout, http.MethodPut, base+"/internal/chunks/"+chunkID, data, hdr)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 201:
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	case 400, 409:
		return fmt.Errorf("%w: PUT %s: %v", ErrChunkRejected, chunkID, httpx.DecodeError(resp))
	}
	return fmt.Errorf("%w: PUT %s -> %d", ErrWorker, chunkID, resp.StatusCode)
}

// GetChunk fetches a chunk's bytes (unverified; the caller checks the hash).
func (c *WorkerClient) GetChunk(ctx context.Context, base, chunkID string) ([]byte, error) {
	resp, cancel, err := c.do(ctx, c.timeout, http.MethodGet, base+"/internal/chunks/"+chunkID, nil, nil)
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("%w: GET %s: %v", ErrWorker, chunkID, err)
		}
		return data, nil
	case 404:
		return nil, fmt.Errorf("%w: %s", ErrChunkMissing, chunkID)
	}
	return nil, fmt.Errorf("%w: GET %s -> %d", ErrWorker, chunkID, resp.StatusCode)
}

// DeleteChunk returns nil when the chunk is gone (deleted now or already absent).
func (c *WorkerClient) DeleteChunk(ctx context.Context, base, chunkID string) error {
	resp, cancel, err := c.do(ctx, c.timeout, http.MethodDelete, base+"/internal/chunks/"+chunkID, nil, nil)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode == 204 || resp.StatusCode == 404 {
		return nil
	}
	return fmt.Errorf("%w: DELETE %s -> %d", ErrWorker, chunkID, resp.StatusCode)
}

func (c *WorkerClient) getJSON(ctx context.Context, timeout time.Duration, method, url string, out any) error {
	resp, cancel, err := c.do(ctx, timeout, method, url, nil, nil)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%w: %s %s: %v", ErrWorker, method, url, httpx.DecodeError(resp))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%w: decode %s: %v", ErrWorker, url, err)
	}
	return nil
}

// Inventory lists what a worker actually holds; verify re-hashes every chunk.
func (c *WorkerClient) Inventory(ctx context.Context, base, workerID string, verify bool) (api.Inventory, error) {
	var inv api.Inventory
	err := c.getJSON(ctx, 2*time.Minute, http.MethodGet,
		fmt.Sprintf("%s/internal/workers/%s/chunks?verify=%t", base, workerID, verify), &inv)
	return inv, err
}

// Scrub asks a worker to re-hash its chunks now.
func (c *WorkerClient) Scrub(ctx context.Context, base string) (api.ScrubResult, error) {
	var res api.ScrubResult
	err := c.getJSON(ctx, 5*time.Minute, http.MethodPost, base+"/internal/scrub", &res)
	return res, err
}
