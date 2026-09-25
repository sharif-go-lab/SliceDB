package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

var ErrNotFound = errors.New("key not found")

// Client talks to the cluster through one or more load balancers. Every operation is retried
// with exponential backoff (rotating over the load balancers) when the connection fails or the
// cluster answers with a transient error, e.g. while a partition fails over.
type Client struct {
	endpoints   []string
	http        *http.Client
	next        atomic.Uint64
	retries     atomic.Int64
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func New(endpoints []string) *Client {
	var clean []string
	for _, e := range endpoints {
		if e = strings.TrimRight(strings.TrimSpace(e), "/"); e != "" {
			if !strings.Contains(e, "://") {
				e = "http://" + e
			}
			clean = append(clean, e)
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 512
	return &Client{
		endpoints:   clean,
		http:        &http.Client{Timeout: 10 * time.Second, Transport: transport},
		MaxRetries:  8,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
	}
}

func (c *Client) Set(ctx context.Context, key, value string) error {
	return c.do(ctx, http.MethodPost, "/set", map[string]string{"key": key, "value": value}, nil)
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	var resp struct {
		Value string `json:"value"`
	}
	if err := c.do(ctx, http.MethodGet, "/get?key="+url.QueryEscape(key), nil, &resp); err != nil {
		return "", err
	}
	return resp.Value, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.do(ctx, http.MethodDelete, "/delete", map[string]string{"key": key}, nil)
}

// Retries returns how many retries this client has made so far.
func (c *Client) Retries() int64 {
	return c.retries.Load()
}

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.code, e.msg)
}

func (c *Client) do(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	if len(c.endpoints) == 0 {
		return errors.New("no load balancer endpoints configured")
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}

	start := c.next.Add(1)
	var lastErr error
	for attempt := 0; ; attempt++ {
		endpoint := c.endpoints[(start+uint64(attempt))%uint64(len(c.endpoints))]
		lastErr = c.once(ctx, method, endpoint+path, payload, out)

		var he *httpError
		switch {
		case lastErr == nil:
			return nil
		case errors.As(lastErr, &he) && he.code == http.StatusNotFound:
			return ErrNotFound
		case errors.As(lastErr, &he) && he.code < 500:
			return lastErr // a bad request will not get better by retrying
		case ctx.Err() != nil:
			return ctx.Err()
		}

		if attempt >= c.MaxRetries {
			return fmt.Errorf("giving up after %d attempts: %w", attempt+1, lastErr)
		}
		c.retries.Add(1)
		delay := c.BaseBackoff << uint(attempt)
		if delay > c.MaxBackoff || delay <= 0 {
			delay = c.MaxBackoff
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (c *Client) once(ctx context.Context, method, target string, payload []byte, out interface{}) error {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &httpError{code: resp.StatusCode, msg: strings.TrimSpace(string(msg))}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
