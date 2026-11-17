// Package client is a Go SDK for the object store's public API. It is used by
// dosctl and by the integration tests.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

// Client talks to the controller only; it never needs worker addresses.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{}}
}

// APIError is returned for any non-2xx response.
type APIError = httpx.APIError

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, httpx.DecodeError(resp)
	}
	return resp, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	ct := ""
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body, ct = bytes.NewReader(b), "application/json"
	}
	resp, err := c.do(ctx, method, path, body, ct)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	return dec.Decode(out)
}

// UploadOptions customises an upload.
type UploadOptions struct {
	ContentType string
	Metadata    map[string]any
}

// Upload streams r as multipart/form-data (field "file", optional "metadata").
func (c *Client) Upload(ctx context.Context, name string, r io.Reader, opt UploadOptions) (*api.UploadResponse, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		err := func() error {
			if len(opt.Metadata) > 0 {
				b, err := json.Marshal(opt.Metadata)
				if err != nil {
					return err
				}
				if err := mw.WriteField("metadata", string(b)); err != nil {
					return err
				}
			}
			h := make(textproto.MIMEHeader)
			h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, name))
			ct := opt.ContentType
			if ct == "" {
				ct = "application/octet-stream"
			}
			h.Set("Content-Type", ct)
			part, err := mw.CreatePart(h)
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, r); err != nil {
				return err
			}
			return mw.Close()
		}()
		pw.CloseWithError(err)
	}()
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/objects", pr, mw.FormDataContentType())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out api.UploadResponse
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	return &out, dec.Decode(&out)
}

// DownloadInfo describes a finished download.
type DownloadInfo struct {
	Bytes         int64
	ContentLength int64
	ContentType   string
	ETag          string // sha256 of the whole object
	Filename      string
}

// Download streams an object into w. A short body is reported as an error.
func (c *Client) Download(ctx context.Context, id string, w io.Writer) (DownloadInfo, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v1/objects/"+url.PathEscape(id), nil, "")
	if err != nil {
		return DownloadInfo{}, err
	}
	defer resp.Body.Close()
	info := DownloadInfo{ContentLength: resp.ContentLength, ContentType: resp.Header.Get("Content-Type"),
		ETag: strings.Trim(resp.Header.Get("ETag"), `"`)}
	if cd := resp.Header.Get("Content-Disposition"); strings.Contains(cd, "filename*=UTF-8''") {
		raw := cd[strings.Index(cd, "filename*=UTF-8''")+len("filename*=UTF-8''"):]
		if fn, err := url.PathUnescape(raw); err == nil {
			info.Filename = fn
		}
	}
	n, err := io.Copy(w, resp.Body)
	info.Bytes = n
	if err != nil {
		return info, fmt.Errorf("download interrupted after %d bytes: %w", n, err)
	}
	if info.ContentLength >= 0 && n != info.ContentLength {
		return info, fmt.Errorf("truncated: got %d of %d bytes", n, info.ContentLength)
	}
	return info, nil
}

func (c *Client) Metadata(ctx context.Context, id string) (*api.ObjectMeta, error) {
	var m api.ObjectMeta
	return &m, c.doJSON(ctx, http.MethodGet, "/api/v1/objects/"+url.PathEscape(id)+"/metadata", nil, &m)
}

// PatchMetadata merge-patches custom metadata; a nil value removes a key.
func (c *Client) PatchMetadata(ctx context.Context, id string, patch map[string]any) (*api.ObjectMeta, error) {
	var m api.ObjectMeta
	return &m, c.doJSON(ctx, http.MethodPatch, "/api/v1/objects/"+url.PathEscape(id)+"/metadata", patch, &m)
}

func (c *Client) List(ctx context.Context, limit, offset int) (*api.ListResponse, error) {
	var l api.ListResponse
	q := url.Values{"limit": {strconv.Itoa(limit)}, "offset": {strconv.Itoa(offset)}}
	return &l, c.doJSON(ctx, http.MethodGet, "/api/v1/objects?"+q.Encode(), nil, &l)
}

func (c *Client) Delete(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodDelete, "/api/v1/objects/"+url.PathEscape(id), nil, nil)
}

func (c *Client) Workers(ctx context.Context) (*api.WorkersResponse, error) {
	var w api.WorkersResponse
	return &w, c.doJSON(ctx, http.MethodGet, "/api/v1/cluster/workers", nil, &w)
}

func (c *Client) Replication(ctx context.Context, details bool) (*api.ReplicationReport, error) {
	var r api.ReplicationReport
	return &r, c.doJSON(ctx, http.MethodGet, "/api/v1/cluster/replication?details="+strconv.FormatBool(details), nil, &r)
}

func (c *Client) Repair(ctx context.Context) (*api.RepairResponse, error) {
	var r api.RepairResponse
	return &r, c.doJSON(ctx, http.MethodPost, "/api/v1/cluster/repair", nil, &r)
}

func (c *Client) Scrub(ctx context.Context) (*api.ScrubResponse, error) {
	var r api.ScrubResponse
	return &r, c.doJSON(ctx, http.MethodPost, "/api/v1/cluster/scrub", nil, &r)
}

func (c *Client) Reconcile(ctx context.Context, workerID string) (*api.ReconcileResponse, error) {
	var r api.ReconcileResponse
	return &r, c.doJSON(ctx, http.MethodPost, "/api/v1/cluster/workers/"+url.PathEscape(workerID)+"/reconcile", nil, &r)
}
