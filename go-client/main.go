// agent-benchmark Go HTTP worker. Python retains replay policy and scheduling.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const protocolVersion = 2
const maxFrame = 128 << 20

type object = map[string]any
type request struct {
	ID      uint64  `json:"id"`
	Method  string  `json:"method"`
	Params  object  `json:"params"`
	Timeout float64 `json:"timeout_sec"`
}
type response struct {
	ID        uint64  `json:"id"`
	Result    any     `json:"result,omitempty"`
	Error     string  `json:"error,omitempty"`
	ErrorKind string  `json:"error_kind,omitempty"`
	Elapsed   float64 `json:"worker_elapsed_sec"`
}

type apiError struct {
	status int
	detail string
}

func safeDetail(detail string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			detail = strings.ReplaceAll(detail, secret, "[redacted]")
		}
	}
	if len(detail) > 8192 {
		detail = detail[:8192] + " [truncated]"
	}
	return detail
}

func (e *apiError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.status, e.detail) }
func errorKind(err error) string {
	var api *apiError
	if errors.As(err, &api) {
		return "api"
	}
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "timeout"
	}
	return "transport"
}

// ProtoJSON may encode integers as numbers or decimal strings. Never turn
// malformed exit codes into successful exits through a permissive cast.
func protoInt(m object, key string, required bool, lo, hi int64) (int, error) {
	v, ok := m[key]
	if !ok {
		if required {
			return 0, fmt.Errorf("missing %s", key)
		}
		return 0, nil
	}
	var n int64
	switch value := v.(type) {
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value < float64(lo) || value > float64(hi) {
			return 0, fmt.Errorf("invalid %s", key)
		}
		n = int64(value)
	case string:
		var err error
		n, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %s", key)
		}
	default:
		return 0, fmt.Errorf("invalid %s", key)
	}
	if n < lo || n > hi {
		return 0, fmt.Errorf("out-of-range %s", key)
	}
	return int(n), nil
}

// exitStatus is the parsed shape of an end-event status string.
type exitStatus struct {
	Code   int    // exit code; -1 when the status is not a clean "exit status N"
	Signal string // signal name when the process died from a signal ("" otherwise)
	Raw    string // the original status string, echoed back for diagnostics
	Known  bool   // true when Code came from a numeric "exit status N"
}

// signalNumbers maps Go's os.Signal names (os.ProcessState.String() prints
// "signal: killed") to POSIX numbers, so a signal death reports the familiar
// 128+N shell convention instead of a bare -1.
var signalNumbers = map[string]int{
	"hangup":                   1,
	"interrupt":                2,
	"quit":                     3,
	"illegal instruction":      4,
	"trace/breakpoint trap":    5,
	"aborted":                  6,
	"bus error":                7,
	"floating point exception": 8,
	"killed":                   9,
	"user defined signal 1":    10,
	"segmentation fault":       11,
	"user defined signal 2":    12,
	"broken pipe":              13,
	"alarm clock":              14,
	"terminated":               15,
}

// parseExitStatus understands the shapes envd >= 0.5.15 puts on end events:
// "exit status N" (os.Exit-style) and "signal: NAME" (killed by a signal).
// Anything else keeps Code = -1 and Known = false so the caller can report an
// unknown exit instead of silently pretending it was a signal death.
func parseExitStatus(status string) exitStatus {
	info := exitStatus{Code: -1, Raw: status}
	if rest, ok := strings.CutPrefix(status, "exit status "); ok {
		if n, err := strconv.Atoi(rest); err == nil {
			info.Code, info.Known = n, true
		}
		return info
	}
	if rest, ok := strings.CutPrefix(status, "signal: "); ok {
		info.Signal = strings.TrimSpace(rest)
		if n, ok := signalNumbers[info.Signal]; ok {
			info.Code = 128 + n
		}
	}
	return info
}

type command struct {
	mu             sync.Mutex
	pid            int
	stdout, stderr bytes.Buffer
	code           int
	signal         string // signal name when the process died from one
	statusRaw      string // raw end-event status string ("" when absent)
	exitUnknown    bool   // status string present but not a recognized shape
	err            error
	done           chan struct{}
	cancel         context.CancelFunc
	sandbox        object
}
type worker struct {
	api, key, envd string
	http           *http.Client
	mu             sync.Mutex
	commands       map[string]*command
	cancels        map[uint64]context.CancelFunc
	seq            atomic.Uint64
}

func str(m object, k string) string  { s, _ := m[k].(string); return s }
func num(m object, k string) float64 { n, _ := m[k].(float64); return n }
func obj(m object, k string) object {
	a, _ := m[k].(map[string]any)
	if a == nil {
		return object{}
	}
	return a
}
func newWorker() *worker {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 4096
	tr.MaxIdleConnsPerHost = 4096
	tr.IdleConnTimeout = 90 * time.Second
	// No MaxConnsPerHost limit: replay scheduling owns concurrency.
	return &worker{api: strings.TrimRight(os.Getenv("E2B_API_URL"), "/"), key: os.Getenv("E2B_API_KEY"), envd: strings.TrimRight(os.Getenv("E2B_SANDBOX_URL"), "/"), http: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, commands: map[string]*command{}, cancels: map[uint64]context.CancelFunc{}}
}
func (w *worker) request(ctx context.Context, method, path string, body any) (any, http.Header, error) {
	v, h, _, err := w.requestStatus(ctx, method, path, body)
	return v, h, err
}
func (w *worker) requestStatus(ctx context.Context, method, path string, body any) (any, http.Header, int, error) {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, w.api+path, bytes.NewReader(raw))
	if err != nil {
		return nil, nil, 0, err
	}
	req.Header.Set("X-API-Key", w.key)
	req.Header.Set("User-Agent", "agent-benchmark-go/1")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.http.Do(req)
	if err != nil {
		return nil, nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxFrame+1))
	if err != nil {
		return nil, nil, 0, err
	}
	if len(b) > maxFrame {
		return nil, nil, 0, errors.New("API response exceeds 128MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if method == "DELETE" && resp.StatusCode == 404 {
			return false, resp.Header, resp.StatusCode, nil
		}
		return nil, resp.Header, resp.StatusCode, &apiError{resp.StatusCode, safeDetail(string(b), w.key)}
	}
	if len(b) == 0 {
		return true, resp.Header, resp.StatusCode, nil
	}
	var v any
	if err = json.Unmarshal(b, &v); err != nil {
		return nil, nil, 0, fmt.Errorf("invalid API JSON: %w", err)
	}
	return v, resp.Header, resp.StatusCode, nil
}
func (w *worker) sandbox(ctx context.Context, path string, body any) (any, error) {
	v, _, err := w.request(ctx, "POST", path, body)
	if err != nil {
		return nil, err
	}
	s, ok := v.(map[string]any)
	if !ok || str(s, "sandboxID") == "" {
		return nil, errors.New("invalid sandbox response: missing sandboxID")
	}
	return s, nil
}
func (w *worker) list(ctx context.Context, metadata object) (any, error) {
	return w.listState(ctx, metadata, "")
}
func (w *worker) listState(ctx context.Context, metadata object, state string) (any, error) {
	all := []any{}
	token := ""
	seen := map[string]bool{}
	for {
		q := url.Values{"limit": {"100"}}
		if state != "" {
			q.Set("state", state)
		}
		if token != "" {
			q.Set("nextToken", token)
		}
		v, h, e := w.request(ctx, "GET", "/v2/sandboxes?"+q.Encode(), nil)
		if e != nil {
			return nil, e
		}
		rows, ok := v.([]any)
		if !ok {
			return nil, errors.New("invalid sandbox list response")
		}
		for _, row := range rows {
			s, ok := row.(map[string]any)
			if !ok {
				return nil, errors.New("invalid sandbox list item")
			}
			match := true
			m := obj(s, "metadata")
			for k, v := range metadata {
				if m[k] != v {
					match = false
					break
				}
			}
			if match {
				all = append(all, s)
			}
		}
		token = h.Get("X-Next-Token")
		if token == "" {
			break
		}
		if seen[token] {
			return nil, errors.New("repeated sandbox pagination token")
		}
		seen[token] = true
	}
	return all, nil
}
func (w *worker) envdURL(s object) string {
	if w.envd != "" {
		return w.envd
	}
	domain := str(s, "domain")
	if domain == "" {
		domain = os.Getenv("E2B_DOMAIN")
	}
	if domain == "" {
		domain = "e2b.app"
	}
	return "https://49983-" + str(s, "sandboxID") + "." + domain
}
func (w *worker) rpcRequest(ctx context.Context, s object, method string, body any, stream bool) (*http.Response, error) {
	b, e := json.Marshal(body)
	if e != nil {
		return nil, e
	}
	ct := "application/json"
	if stream {
		framed := make([]byte, 5+len(b))
		binary.BigEndian.PutUint32(framed[1:5], uint32(len(b)))
		copy(framed[5:], b)
		b = framed
		ct = "application/connect+json"
	}
	r, e := http.NewRequestWithContext(ctx, "POST", w.envdURL(s)+"/process.Process/"+method, bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	r.Header.Set("Content-Type", ct)
	r.Header.Set("Connect-Protocol-Version", "1")
	r.Header.Set("E2b-Sandbox-Id", str(s, "sandboxID"))
	r.Header.Set("E2b-Sandbox-Port", "49983")
	if token := str(s, "envdAccessToken"); token != "" {
		r.Header.Set("X-Access-Token", token)
	}
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("root:")))
	resp, e := w.http.Do(r)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, &apiError{resp.StatusCode, safeDetail(string(b), w.key, str(s, "envdAccessToken"))}
	}
	return resp, nil
}
func frame(r io.Reader) (object, bool, error) {
	h := make([]byte, 5)
	if _, e := io.ReadFull(r, h); e != nil {
		return nil, false, e
	}
	if h[0]&1 != 0 {
		return nil, false, errors.New("compressed Connect frames are not supported")
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n > maxFrame {
		return nil, false, errors.New("Connect frame exceeds 128MiB")
	}
	b := make([]byte, int(n))
	if _, e := io.ReadFull(r, b); e != nil {
		return nil, false, e
	}
	var v object
	if e := json.Unmarshal(b, &v); e != nil {
		return nil, false, e
	}
	if h[0]&2 != 0 {
		if v["error"] != nil {
			return nil, true, fmt.Errorf("Connect stream error: %v", v["error"])
		}
		return v, true, nil
	}
	return v, false, nil
}
func (w *worker) start(ctx context.Context, p object) (any, error) {
	// Opening the stream and waiting for it have independent deadlines.
	streamCtx, cancel := context.WithCancel(context.Background())
	timeout := num(p, "command_timeout_sec")
	if timeout > 0 {
		cancel()
		streamCtx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout*float64(time.Second)))
	}
	c := &command{done: make(chan struct{}), cancel: cancel, sandbox: obj(p, "sandbox")}
	started := make(chan error, 1)
	go func() {
		defer close(c.done)
		defer cancel()
		err := func() error {
			resp, e := w.rpcRequest(streamCtx, c.sandbox, "Start", object{"process": object{"cmd": "/bin/bash", "args": []string{"-l", "-c", str(p, "command")}}, "stdin": false}, true)
			if e != nil {
				return e
			}
			defer resp.Body.Close()
			gotStart, gotEnd := false, false
			for {
				v, end, e := frame(resp.Body)
				if e != nil {
					return e
				}
				if end {
					if !gotEnd {
						return errors.New("envd stream ended without process exit")
					}
					return nil
				}
				event := obj(v, "event")
				if sv, ok := event["start"].(map[string]any); ok {
					if gotStart {
						return errors.New("duplicate envd start")
					}
					pid, err := protoInt(sv, "pid", true, 1, math.MaxUint32)
					if err != nil {
						return err
					}
					c.pid = pid
					gotStart = true
					started <- nil
				}
				if data, ok := event["data"].(map[string]any); ok {
					c.mu.Lock()
					for key, dst := range map[string]*bytes.Buffer{"stdout": &c.stdout, "stderr": &c.stderr} {
						if s := str(data, key); s != "" {
							raw, e := base64.StdEncoding.DecodeString(s)
							if e != nil {
								c.mu.Unlock()
								return e
							}
							if c.stdout.Len()+c.stderr.Len()+len(raw) > maxFrame {
								c.mu.Unlock()
								return errors.New("command output exceeds 128MiB")
							}
							dst.Write(raw)
						}
					}
					c.mu.Unlock()
				}
				if ev, ok := event["end"].(map[string]any); ok {
					if !gotStart {
						return errors.New("process exit before start")
					}
					// A process end event also covers signal exits. Like the SDK,
					// use its exit code rather than treating exited=false as a
					// transport failure (for example, a command killing itself).
					if alias, ok := ev["exit_code"]; ok {
						if _, both := ev["exitCode"]; both {
							return errors.New("duplicate exit code fields")
						}
						ev["exitCode"] = alias
					}
					// envd >= 0.5.15 reports the end event as {"exited": true,
					// "status": "exit status N"} (a Go ExitError string) with no
					// numeric field; older envds emit exitCode/exit_code directly.
					// Parse the status string so both shapes yield a code; signal
					// deaths keep the signal name and use the 128+N convention,
					// and unrecognized strings are flagged as unknown exits rather
					// than being silently conflated with a signal death.
					// (float64: protoInt only accepts JSON-number shapes.)
					if _, ok := ev["exitCode"]; !ok {
						if status := str(ev, "status"); status != "" {
							info := parseExitStatus(status)
							ev["exitCode"] = float64(info.Code)
							c.signal = info.Signal
							c.statusRaw = info.Raw
							c.exitUnknown = !info.Known && info.Signal == ""
						}
					}
					code, err := protoInt(ev, "exitCode", true, math.MinInt32, math.MaxInt32)
					if err != nil {
						return err
					}
					c.code = code
					gotEnd = true
				}
			}
		}()
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		if err != nil {
			select {
			case started <- err:
			default:
			}
		}
	}()
	select {
	case e := <-started:
		if e != nil {
			cancel()
			return nil, e
		}
		id := fmt.Sprint(w.seq.Add(1))
		w.mu.Lock()
		w.commands[id] = c
		w.mu.Unlock()
		return object{"handle": id, "pid": c.pid}, nil
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
}
func (w *worker) handle(ctx context.Context, method string, p object) (any, error) {
	id := str(p, "handle")
	w.mu.Lock()
	c := w.commands[id]
	w.mu.Unlock()
	if c == nil {
		return nil, errors.New("unknown command handle")
	}
	if method == "command_wait" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.err != nil {
			return nil, c.err
		}
		w.mu.Lock()
		delete(w.commands, id)
		w.mu.Unlock()
		result := object{"stdout": c.stdout.String(), "stderr": c.stderr.String(), "exit_code": c.code}
		if c.signal != "" {
			result["exit_signal"] = c.signal
		}
		if c.exitUnknown {
			result["exit_unknown"] = true
			result["exit_status_raw"] = c.statusRaw
		}
		return result, nil
	}
	resp, e := w.rpcRequest(ctx, c.sandbox, "SendSignal", object{"process": object{"pid": c.pid}, "signal": 9}, false)
	if e != nil {
		// SendSignal failed (e.g. 404: the process already exited). The
		// stream may have ended on its own; regardless, the handle must not
		// leak -- cancel the stream and drop the entry so a later wait/kill
		// cannot wedge on the stale goroutine.
		c.cancel()
		w.mu.Lock()
		delete(w.commands, id)
		w.mu.Unlock()
		return nil, e
	}
	_, e = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	c.cancel()
	w.mu.Lock()
	delete(w.commands, id)
	w.mu.Unlock()
	return true, e
}
func (w *worker) call(ctx context.Context, r request) (any, error) {
	p := r.Params
	id := url.PathEscape(str(p, "sandbox_id"))
	switch r.Method {
	case "version":
		return object{"protocol": protocolVersion, "go_version": runtime.Version()}, nil
	case "create":
		return w.sandbox(ctx, "/sandboxes", object{"templateID": p["template"], "timeout": p["timeout"], "secure": true, "allow_internet_access": true, "envVars": object{}, "metadata": p["metadata"]})
	case "resume":
		return w.sandbox(ctx, "/sandboxes/"+id+"/connect", object{"timeout": p["timeout"]})
	case "pause":
		v, _, e := w.request(ctx, "POST", "/sandboxes/"+id+"/pause", object{"memory": true})
		return v, e
	case "delete":
		v, _, e := w.request(ctx, "DELETE", "/sandboxes/"+id, nil)
		if e == nil {
			w.mu.Lock()
			for key, c := range w.commands {
				if str(c.sandbox, "sandboxID") == str(p, "sandbox_id") {
					c.cancel()
					delete(w.commands, key)
				}
			}
			w.mu.Unlock()
		}
		return v, e
	case "list":
		return w.list(ctx, obj(p, "metadata"))
	case "command_start":
		return w.start(ctx, p)
	case "command_wait", "command_kill":
		return w.handle(ctx, r.Method, p)
	default:
		return nil, fmt.Errorf("unknown method %q", r.Method)
	}
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		json.NewEncoder(os.Stdout).Encode(object{"protocol": protocolVersion, "go_version": runtime.Version()})
		return
	}
	w := newWorker()
	defer w.http.CloseIdleConnections()
	var output sync.Mutex
	var wg sync.WaitGroup
	scan := bufio.NewScanner(os.Stdin)
	scan.Buffer(make([]byte, 65536), 8<<20)
	for scan.Scan() {
		var r request
		if e := json.Unmarshal(scan.Bytes(), &r); e != nil {
			fmt.Fprintln(os.Stderr, "invalid worker request")
			break
		}
		if r.Method == "cancel" {
			w.mu.Lock()
			cancel := w.cancels[uint64(num(r.Params, "id"))]
			w.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		if r.Timeout > 0 {
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), time.Duration(r.Timeout*float64(time.Second)))
		}
		w.mu.Lock()
		w.cancels[r.ID] = cancel
		w.mu.Unlock()
		wg.Add(1)
		go func(r request, ctx context.Context, cancel context.CancelFunc) {
			defer wg.Done()
			defer cancel()
			start := time.Now()
			v, e := w.call(ctx, r)
			reply := response{ID: r.ID, Result: v, Elapsed: time.Since(start).Seconds()}
			if e != nil {
				reply.Result = nil
				reply.Error = safeDetail(e.Error(), w.key, str(obj(r.Params, "sandbox"), "envdAccessToken"))
				reply.ErrorKind = errorKind(e)
			}
			w.mu.Lock()
			delete(w.cancels, r.ID)
			w.mu.Unlock()
			output.Lock()
			json.NewEncoder(os.Stdout).Encode(reply)
			output.Unlock()
		}(r, ctx, cancel)
	}
	w.mu.Lock()
	for _, cancel := range w.cancels {
		cancel()
	}
	for _, c := range w.commands {
		c.cancel()
	}
	w.mu.Unlock()
	wg.Wait()
	if e := scan.Err(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
