package client

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

	"s7s/pkg/api"
)

type Client struct {
	baseURL string
	http    *http.Client // has a timeoutm for normal requests
	stream  *http.Client // no timeout, for watches
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
		stream:  &http.Client{},
	}
}

// APIError is returned only when the server responds with a non-2xx status
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("server returned %d: %s", e.StatusCode, e.Message)
}

// helper for not found
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.StatusCode == http.StatusNotFound
}

// helper for getting podsURL
func (c *Client) podsURL(ns string) string {
	return fmt.Sprintf("%s/api/v1/namespaces/%s/pods", c.baseURL, ns)
}

// do sends one request. body and out may still be nil
// this is the main central function that handles all the hardwork
func (c *Client) do(method, url string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach the server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return &APIError{StatusCode: resp.StatusCode, Message: e.Error}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *Client) CreatePod(ns string, p *api.Pod) (*api.Pod, error) {
	var out api.Pod

	if err := c.do(http.MethodPost, c.podsURL(ns), p, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) UpdatePod(ns string, p *api.Pod) (*api.Pod, error) {
	var out api.Pod

	if err := c.do(http.MethodPut, c.podsURL(ns)+"/"+p.Metadata.Name, p, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetPod(ns, name string) (*api.Pod, error) {
	var out api.Pod

	if err := c.do(http.MethodGet, c.podsURL(ns)+"/"+name, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListPods(ns string) (*api.PodList, error) {
	var out api.PodList

	if err := c.do(http.MethodGet, c.podsURL(ns), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) DeletePod(ns, name string) error {
	return c.do(http.MethodDelete, c.podsURL(ns)+"/"+name, nil, nil)
}

func (c *Client) WatchPods(ctx context.Context, ns string) (<-chan api.PodEvent, error) {
	url := c.podsURL(ns) + "?watch=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.stream.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach server: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &APIError{StatusCode: resp.StatusCode, Message: "watch failed"}
	}

	out := make(chan api.PodEvent)
	go func() {
		defer close(out)
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)
		for {
			var ev api.PodEvent

			if err := dec.Decode(&ev); err != nil {
				return // EOF, cancelled, or bad data
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
