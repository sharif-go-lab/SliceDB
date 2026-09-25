package network

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// StatusError is returned when the remote side answered with a non-200 status.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("received non-200 response: %d %s", e.Code, strings.TrimSpace(e.Body))
}

// StatusCode returns the HTTP status carried by err, or 0 if err is not a StatusError.
func StatusCode(err error) int {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code
	}
	return 0
}

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return NewClientWithTimeout(5 * time.Second)
}

func NewClientWithTimeout(timeout time.Duration) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 256
	return &Client{
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

func (c *Client) HTTP() *http.Client {
	return c.httpClient
}

// Do sends data (if not nil) as JSON and decodes a 200 response into response (if not nil).
func (c *Client) Do(ctx context.Context, method, url string, data interface{}, response interface{}) error {
	var body io.Reader
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &StatusError{Code: resp.StatusCode, Body: string(msg)}
	}

	if response != nil {
		return json.NewDecoder(resp.Body).Decode(response)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) Post(url string, data interface{}, response interface{}) error {
	return c.Do(context.Background(), http.MethodPost, url, data, response)
}

func (c *Client) Get(url string, response interface{}) error {
	return c.Do(context.Background(), http.MethodGet, url, nil, response)
}

func (c *Client) Delete(url string, data interface{}, response interface{}) error {
	return c.Do(context.Background(), http.MethodDelete, url, data, response)
}
