package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func emit(w http.ResponseWriter, v any, end bool) {
	b, _ := json.Marshal(v)
	h := make([]byte, 5)
	if end {
		h[0] = 2
	}
	binary.BigEndian.PutUint32(h[1:], uint32(len(b)))
	w.Write(h)
	w.Write(b)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}
func fixture(t *testing.T, fn http.HandlerFunc) *worker {
	t.Helper()
	srv := httptest.NewServer(fn)
	t.Cleanup(srv.Close)
	t.Setenv("E2B_API_URL", srv.URL)
	t.Setenv("E2B_SANDBOX_URL", srv.URL)
	t.Setenv("E2B_API_KEY", "test-key")
	w := newWorker()
	t.Cleanup(w.http.CloseIdleConnections)
	return w
}
func TestLifecycleAndPaginatedReconciliation(t *testing.T) {
	calls := []string{}
	w := fixture(t, func(rw http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Error("missing auth")
		}
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch {
		case r.URL.Path == "/sandboxes":
			var b object
			json.NewDecoder(r.Body).Decode(&b)
			if str(b, "templateID") != "fixture" || b["secure"] != true {
				t.Error(b)
			}
			fmt.Fprint(rw, `{"sandboxID":"s1"}`)
		case strings.HasSuffix(r.URL.Path, "/connect"):
			fmt.Fprint(rw, `{"sandboxID":"s1","envdAccessToken":"secret"}`)
		case strings.HasSuffix(r.URL.Path, "/pause"):
			var b object
			json.NewDecoder(r.Body).Decode(&b)
			if b["memory"] != true {
				t.Error(b)
			}
			rw.WriteHeader(204)
		case r.Method == "DELETE":
			rw.WriteHeader(204)
		case r.URL.Path == "/v2/sandboxes":
			if r.URL.Query().Get("nextToken") == "" {
				rw.Header().Set("X-Next-Token", "page2")
				fmt.Fprint(rw, `[{"sandboxID":"other","metadata":{"key":"no"}}]`)
			} else {
				fmt.Fprint(rw, `[{"sandboxID":"s1","metadata":{"key":"yes"}}]`)
			}
		default:
			t.Error(r.URL)
			rw.WriteHeader(404)
		}
	})
	for _, r := range []request{{Method: "create", Params: object{"template": "fixture", "timeout": 86400, "metadata": object{"key": "yes"}}}, {Method: "pause", Params: object{"sandbox_id": "s1"}}, {Method: "resume", Params: object{"sandbox_id": "s1", "timeout": 86400}}, {Method: "delete", Params: object{"sandbox_id": "s1"}}} {
		if _, e := w.call(context.Background(), r); e != nil {
			t.Fatal(e)
		}
	}
	result, e := w.list(context.Background(), object{"key": "yes"})
	if e != nil || len(result.([]any)) != 1 {
		t.Fatal(result, e)
	}
	if len(calls) != 6 {
		t.Fatal(calls)
	}
}
func TestCommandStreamCompleteResponseAndNonzero(t *testing.T) {
	w := fixture(t, func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/process.Process/Start" || r.Header.Get("E2b-Sandbox-Id") != "s1" || r.Header.Get("X-Access-Token") != "token" || r.Header.Get("Authorization") != "Basic cm9vdDo=" {
			t.Error(r.URL, r.Header)
		}
		v, end, e := frame(r.Body)
		if e != nil || end {
			t.Error(e)
		}
		process := obj(v, "process")
		if str(process, "cmd") != "/bin/bash" || v["stdin"] != false {
			t.Error(v)
		}
		rw.Header().Set("Content-Type", "application/connect+json")
		emit(rw, object{"event": object{"start": object{"pid": 42}}}, false)
		for _, b := range [][]byte{{0xe4}, {0xbd, 0xa0}, []byte("\n")} {
			emit(rw, object{"event": object{"data": object{"stdout": base64.StdEncoding.EncodeToString(b)}}}, false)
		}
		emit(rw, object{"event": object{"data": object{"stderr": base64.StdEncoding.EncodeToString([]byte("failed"))}}}, false)
		emit(rw, object{"event": object{"end": object{"exitCode": 7, "exited": true, "error": "exit status 7"}}}, false)
		emit(rw, object{}, true)
	})
	v, e := w.start(context.Background(), object{"sandbox": object{"sandboxID": "s1", "envdAccessToken": "token"}, "command": "printf hello"})
	if e != nil {
		t.Fatal(e)
	}
	result, e := w.handle(context.Background(), "command_wait", v.(object))
	if e != nil {
		t.Fatal(e)
	}
	m := result.(object)
	if m["stdout"] != "你\n" || m["stderr"] != "failed" || m["exit_code"] != 7 {
		t.Fatal(m)
	}
}
func TestDisconnectedStreamIsNotSuccessfulExit(t *testing.T) {
	w := fixture(t, func(rw http.ResponseWriter, r *http.Request) {
		emit(rw, object{"event": object{"start": object{"pid": 3}}}, false)
	})
	v, e := w.start(context.Background(), object{"sandbox": object{"sandboxID": "s1"}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.handle(context.Background(), "command_wait", v.(object)); e == nil {
		t.Fatal("lost stream became a successful command")
	}
}
func TestWaitCancellationRetainsHandleForKill(t *testing.T) {
	killed := make(chan struct{})
	w := fixture(t, func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/SendSignal") {
			var m object
			json.NewDecoder(r.Body).Decode(&m)
			if num(m, "signal") != 9 {
				t.Error(m)
			}
			close(killed)
			fmt.Fprint(rw, `{}`)
			return
		}
		emit(rw, object{"event": object{"start": object{"pid": 4}}}, false)
		<-r.Context().Done()
	})
	v, e := w.start(context.Background(), object{"sandbox": object{"sandboxID": "s1"}})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, e = w.handle(ctx, "command_wait", v.(object)); e == nil {
		t.Fatal("expected cancellation")
	}
	if _, e = w.handle(context.Background(), "command_kill", v.(object)); e != nil {
		t.Fatal(e)
	}
	<-killed
}
func TestConnectErrorEnvelope(t *testing.T) {
	var b bytes.Buffer
	emitBuffer := httptest.NewRecorder()
	emit(emitBuffer, object{"error": object{"code": "unavailable", "message": "disconnected"}}, true)
	b.Write(emitBuffer.Body.Bytes())
	if _, _, e := frame(&b); e == nil {
		t.Fatal("Connect error ignored")
	}
}
func TestAmbiguousCreateIsNotRetriedByTransport(t *testing.T) {
	var calls atomic.Int32
	w := fixture(t, func(rw http.ResponseWriter, r *http.Request) { calls.Add(1); panic(http.ErrAbortHandler) })
	_, e := w.call(context.Background(), request{Method: "create", Params: object{"template": "fixture"}})
	if e == nil || calls.Load() != 1 {
		t.Fatal("ambiguous create replayed", calls.Load(), e)
	}
}
func TestCommandDeadlineCanStillKillProcess(t *testing.T) {
	w := fixture(t, func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/SendSignal") {
			fmt.Fprint(rw, `{}`)
			return
		}
		emit(rw, object{"event": object{"start": object{"pid": 5}}}, false)
		<-r.Context().Done()
	})
	v, e := w.start(context.Background(), object{"sandbox": object{"sandboxID": "s1"}, "command_timeout_sec": .02})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.handle(context.Background(), "command_wait", v.(object)); e == nil {
		t.Fatal("expected deadline")
	}
	if _, e = w.handle(context.Background(), "command_kill", v.(object)); e != nil {
		t.Fatal(e)
	}
}

func TestProtoIntegerValidation(t *testing.T) {
	for _, v := range []any{float64(7), "7"} {
		n, e := protoInt(object{"code": v}, "code", false, -2147483648, 2147483647)
		if e != nil || n != 7 {
			t.Fatal(n, e)
		}
	}
	for _, v := range []any{"invalid", nil, 7.5, 2147483648.0} {
		if _, e := protoInt(object{"code": v}, "code", false, -2147483648, 2147483647); e == nil {
			t.Fatal("invalid value silently accepted", v)
		}
	}
	if n, e := protoInt(object{}, "code", false, -2147483648, 2147483647); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if _, e := protoInt(object{}, "pid", true, 1, 4294967295); e == nil {
		t.Fatal("missing PID accepted")
	}
}

func TestErrorKindsDoNotInspectServerMessage(t *testing.T) {
	if errorKind(&apiError{400, "deadline exceeded"}) != "api" {
		t.Fatal("API body became timeout")
	}
	if errorKind(context.DeadlineExceeded) != "timeout" {
		t.Fatal("deadline classification")
	}
	if errorKind(errors.New("unexpected EOF")) != "transport" {
		t.Fatal("stream classification")
	}
}
