package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// dockerAPI talks to the Docker Engine over its unix socket.
//
// The control plane used to fork the docker CLI for every question: two
// `docker inspect` processes per node per snapshot, fourteen per build. On the
// Oracle host one fork cost 10-30ms against a 0.08-CPU cap, so the container
// was throttled in every CPU period and a snapshot took seconds. The same
// question over the socket costs 1-2ms, and one list call answers all seven.
type dockerAPI struct {
	client *http.Client
}

const dockerRequestTimeout = 15 * time.Second

func newDockerAPI(socket string) *dockerAPI {
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		MaxIdleConns:        4,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
	}
	return &dockerAPI{client: &http.Client{Transport: tr, Timeout: dockerRequestTimeout}}
}

// containerState is what the engine needs to know about one container.
type containerState struct {
	Running  bool
	Status   string // Docker's state word: running, exited, created, ...
	Networks []string
}

// containers lists every container in one call, keyed by name without
// Docker's leading slash.
func (d *dockerAPI) containers(ctx context.Context) (map[string]containerState, error) {
	var list []struct {
		Names           []string
		State           string
		NetworkSettings struct {
			Networks map[string]json.RawMessage
		}
	}
	if err := d.do(ctx, http.MethodGet, "/containers/json?all=1", nil, &list, http.StatusOK); err != nil {
		return nil, err
	}
	out := make(map[string]containerState, len(list))
	for _, c := range list {
		st := containerState{
			// Matches inspect's State.Running, which is also true while a
			// container is paused or restarting.
			Running:  c.State == "running" || c.State == "paused" || c.State == "restarting",
			Status:   c.State,
			Networks: networkNames(c.NetworkSettings.Networks),
		}
		for _, name := range c.Names {
			out[strings.TrimPrefix(name, "/")] = st
		}
	}
	return out, nil
}

// inspect reads one container. Used on the rare single-node paths.
func (d *dockerAPI) inspect(ctx context.Context, name string) (containerState, error) {
	var body struct {
		State struct {
			Running bool
			Status  string
		}
		NetworkSettings struct {
			Networks map[string]json.RawMessage
		}
	}
	if err := d.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(name)+"/json", nil, &body, http.StatusOK); err != nil {
		return containerState{}, err
	}
	return containerState{
		Running:  body.State.Running,
		Status:   body.State.Status,
		Networks: networkNames(body.NetworkSettings.Networks),
	}, nil
}

// stop gives the container timeoutSec to exit, then kills it. 304 means it
// was already stopped, which is what the caller wanted.
func (d *dockerAPI) stop(ctx context.Context, name string, timeoutSec int) error {
	path := "/containers/" + url.PathEscape(name) + "/stop?t=" + strconv.Itoa(timeoutSec)
	return d.do(ctx, http.MethodPost, path, nil, nil, http.StatusNoContent, http.StatusNotModified)
}

// start starts the container. 304 means it was already running.
func (d *dockerAPI) start(ctx context.Context, name string) error {
	path := "/containers/" + url.PathEscape(name) + "/start"
	return d.do(ctx, http.MethodPost, path, nil, nil, http.StatusNoContent, http.StatusNotModified)
}

func (d *dockerAPI) networkConnect(ctx context.Context, network, name string) error {
	path := "/networks/" + url.PathEscape(network) + "/connect"
	return d.do(ctx, http.MethodPost, path, map[string]string{"Container": name}, nil, http.StatusOK)
}

func (d *dockerAPI) networkDisconnect(ctx context.Context, network, name string) error {
	path := "/networks/" + url.PathEscape(network) + "/disconnect"
	return d.do(ctx, http.MethodPost, path, map[string]string{"Container": name}, nil, http.StatusOK)
}

// do sends one request and decodes the reply into out when the status is one
// of ok. Any other status becomes an error carrying the daemon's message, so
// callers can still match on text such as "already".
func (d *dockerAPI) do(ctx context.Context, method, path string, body, out any, ok ...int) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	// The host is ignored: the transport always dials the socket.
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("controlplane: docker %s %s: %w", method, path, err)
	}
	defer res.Body.Close()
	for _, code := range ok {
		if res.StatusCode != code {
			continue
		}
		if out == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			return nil
		}
		if err := json.NewDecoder(res.Body).Decode(out); err != nil {
			return fmt.Errorf("controlplane: docker %s %s: decode: %w", method, path, err)
		}
		return nil
	}
	return fmt.Errorf("controlplane: docker %s %s: %d %s", method, path, res.StatusCode, daemonMessage(res.Body))
}

// daemonMessage pulls {"message": "..."} out of an error reply, falling back
// to the raw text.
func daemonMessage(r io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(r, 4096))
	var e struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &e) == nil && e.Message != "" {
		return e.Message
	}
	return strings.TrimSpace(string(raw))
}

func networkNames(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for name := range m {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
