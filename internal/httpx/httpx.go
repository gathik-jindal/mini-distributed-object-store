// Package httpx has the JSON read/write helpers, the uniform error envelope and
// the APIError type that maps domain errors onto HTTP status codes.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
)

// APIError is an error that knows its HTTP status and machine-readable code.
type APIError struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
}

func (e *APIError) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message) }

// WithDetails returns the error with details attached.
func (e *APIError) WithDetails(d map[string]any) *APIError { e.Details = d; return e }

func newErr(status int, code, format string, args ...any) *APIError {
	return &APIError{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func InvalidRequest(f string, a ...any) *APIError { return newErr(400, "INVALID_REQUEST", f, a...) }
func ObjectNotFound(id string) *APIError {
	return newErr(404, "OBJECT_NOT_FOUND", "object %s not found", id)
}
func NotFound(f string, a ...any) *APIError   { return newErr(404, "NOT_FOUND", f, a...) }
func ObjectBusy(f string, a ...any) *APIError { return newErr(409, "OBJECT_BUSY", f, a...) }
func Conflict(f string, a ...any) *APIError   { return newErr(409, "CONFLICT", f, a...) }
func InsufficientWorkers(f string, a ...any) *APIError {
	return newErr(503, "INSUFFICIENT_WORKERS", f, a...)
}
func ChunkUnavailable(f string, a ...any) *APIError {
	return newErr(503, "CHUNK_UNAVAILABLE", f, a...)
}
func DataCorrupted(f string, a ...any) *APIError { return newErr(500, "DATA_CORRUPTED", f, a...) }
func Internal(f string, a ...any) *APIError      { return newErr(500, "INTERNAL_ERROR", f, a...) }

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

// WriteError writes any error as the JSON envelope; non-APIErrors become 500.
func WriteError(w http.ResponseWriter, err error) {
	var ae *APIError
	if !errors.As(err, &ae) {
		slog.Error("unhandled error", "error", err)
		ae = Internal("%v", err)
	}
	details := ae.Details
	if details == nil {
		details = map[string]any{}
	}
	WriteJSON(w, ae.Status, api.ErrorEnvelope{Error: api.ErrorBody{Code: ae.Code, Message: ae.Message, Details: details}})
}

// ReadJSON decodes a JSON body into v; malformed JSON is a 400.
func ReadJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return InvalidRequest("body must be valid JSON: %v", err)
	}
	return nil
}

// HandlerFunc is an http handler that returns an error instead of writing it.
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// Handle adapts a HandlerFunc, rendering returned errors as the JSON envelope.
func Handle(fn HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			WriteError(w, err)
		}
	}
}

// DecodeError turns a non-2xx response body into an *APIError (best effort).
func DecodeError(resp *http.Response) *APIError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var env api.ErrorEnvelope
	if json.Unmarshal(body, &env) == nil && env.Error.Code != "" {
		return &APIError{Status: resp.StatusCode, Code: env.Error.Code, Message: env.Error.Message, Details: env.Error.Details}
	}
	msg := string(body)
	if len(msg) > 300 {
		msg = msg[:300]
	}
	return &APIError{Status: resp.StatusCode, Code: http.StatusText(resp.StatusCode), Message: msg}
}

// NotFoundHandler renders unknown routes as the JSON envelope.
func NotFoundHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, NotFound("no route for %s %s", r.Method, r.URL.Path))
	}
}

// SetupLogging configures slog as key=value text on stdout, tagged by component.
func SetupLogging(level, component string) {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(level))
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h).With("component", component))
}
