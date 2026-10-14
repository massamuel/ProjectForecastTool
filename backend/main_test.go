package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func request(g *gateway, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:8765"+path, strings.NewReader(body))
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}
func poll(t *testing.T, g *gateway, id, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		w := request(g, "GET", "/api/jobs/"+id, "")
		var j job
		json.Unmarshal(w.Body.Bytes(), &j)
		if j.Status == want {
			return
		}
		time.Sleep(time.Millisecond * 5)
	}
	t.Fatalf("job did not reach %s: %s", want, request(g, "GET", "/api/jobs/"+id, "").Body.String())
}
func accepted(t *testing.T, g *gateway) string {
	t.Helper()
	w := request(g, "POST", "/api/ingest", `{"project_id":1,"documents":[{"name":"brief","text":"scope pivot"}]}`)
	if w.Code != 202 {
		t.Fatalf("submit: %d %s", w.Code, w.Body.String())
	}
	var j job
	json.Unmarshal(w.Body.Bytes(), &j)
	return j.ID
}
func TestQueueCapacityCancellationAndWorkerBound(t *testing.T) {
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	var active, peak atomic.Int32
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Token") != "test" {
			t.Error("missing internal token")
		}
		if r.URL.Path == "/api/state" {
			io.WriteString(w, `{"projects":[{"id":1}]}`)
			return
		}
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer active.Add(-1)
		started <- struct{}{}
		<-release
		io.WriteString(w, `{"chunks":3}`)
	}))
	defer service.Close()
	g := newGateway(service.URL, "test", nil, 1, 2, time.Second*2)
	defer g.close()
	first := accepted(t, g)
	<-started
	second := accepted(t, g)
	w := request(g, "POST", "/api/ingest", `{"project_id":1,"documents":[{"name":"x","text":"y"}]}`)
	if w.Code != 429 {
		t.Fatalf("expected backpressure, got %d", w.Code)
	}
	if request(g, "DELETE", "/api/jobs/"+first, "").Code != 409 {
		t.Error("running job should not claim cancellation")
	}
	if request(g, "DELETE", "/api/jobs/"+second, "").Code != 200 {
		t.Error("queued job cancellation failed")
	}
	close(release)
	poll(t, g, first, "succeeded")
	poll(t, g, second, "cancelled")
	if peak.Load() != 1 {
		t.Errorf("exceeded worker limit: %d", peak.Load())
	}
}
func TestTwoWorkersActuallyOverlap(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/state" {
			io.WriteString(w, `{"projects":[{"id":1}]}`)
			return
		}
		started <- struct{}{}
		<-release
		io.WriteString(w, `{"chunks":1}`)
	}))
	defer service.Close()
	g := newGateway(service.URL, "test", nil, 2, 2, time.Second*2)
	defer g.close()
	first := accepted(t, g)
	second := accepted(t, g)
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("workers did not overlap")
		}
	}
	close(release)
	poll(t, g, first, "succeeded")
	poll(t, g, second, "succeeded")
}
func TestRetriesOnlyReads(t *testing.T) {
	var calls atomic.Int32
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		io.WriteString(w, `{"error":"busy"}`)
	}))
	defer service.Close()
	g := newGateway(service.URL, "test", nil, 1, 1, time.Second)
	defer g.close()
	if _, _, err := g.call(context.Background(), "GET", "/api/state", nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("GET did not retry")
	}
	calls.Store(0)
	g.call(context.Background(), "POST", "/api/time", []byte(`{}`))
	if calls.Load() != 1 {
		t.Fatal("unsafe write retry")
	}
}
func TestInputAndOriginGuards(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "test", []byte("dashboard"), 1, 1, time.Millisecond*50)
	defer g.close()
	cases := []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/", "", 200}, {"POST", "/api/ingest", "[]", 400}, {"POST", "/api/ingest", `{"project_id":1,"documents":[]}`, 400},
		{"POST", "/api/projects", strings.Repeat("x", maxBody+1), 413}, {"GET", "/api/projects", "", 405}, {"GET", "/missing", "", 404},
	}
	for _, c := range cases {
		w := request(g, c.method, c.path, c.body)
		if w.Code != c.status {
			t.Errorf("%s: got %d expected %d", c.path, w.Code, c.status)
		}
	}
	for _, host := range []string{"evil.example", "127.0.0.1.evil.example"} {
		r := httptest.NewRequest("GET", "http://"+host+"/", nil)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Error("untrusted host accepted")
		}
	}
	r := httptest.NewRequest("POST", "http://127.0.0.1/api/demo", bytes.NewBufferString(`{}`))
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Error("cross origin accepted")
	}
}
func TestClientCancellationPropagates(t *testing.T) {
	stopped := make(chan struct{})
	started := make(chan struct{})
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(stopped) }))
	defer service.Close()
	g := newGateway(service.URL, "test", nil, 1, 1, time.Second)
	defer g.close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.call(ctx, "GET", "/api/state", nil); close(done) }()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive cancellation")
	}
}
func TestIngestFailureAndUnknownCommit(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/state" {
			io.WriteString(w, `{"projects":[{"id":1}]}`)
			return
		}
		w.WriteHeader(400)
		io.WriteString(w, `{"error":"bad document"}`)
	}))
	defer service.Close()
	g := newGateway(service.URL, "test", nil, 1, 1, time.Second)
	defer g.close()
	id := accepted(t, g)
	poll(t, g, id, "failed")
	// A network timeout cannot prove whether the Python transaction committed.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/state" {
			io.WriteString(w, `{"projects":[{"id":1}]}`)
			return
		}
		time.Sleep(100 * time.Millisecond)
		io.WriteString(w, `{"chunks":1}`)
	}))
	defer slow.Close()
	h := newGateway(slow.URL, "test", nil, 1, 1, 30*time.Millisecond)
	defer h.close()
	id = accepted(t, h)
	poll(t, h, id, "unknown")
}

func TestShutdownStopsQueuedJobs(t *testing.T) {
	started := make(chan struct{})
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/state" {
			io.WriteString(w, `{"projects":[{"id":1}]}`)
			return
		}
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer service.Close()
	g := newGateway(service.URL, "test", nil, 1, 2, time.Second)
	first := accepted(t, g)
	<-started
	second := accepted(t, g)
	g.close()
	poll(t, g, first, "unknown")
	poll(t, g, second, "cancelled")
	if request(g, "POST", "/api/ingest", `{"project_id":1,"documents":[{"name":"x","text":"y"}]}`).Code != 503 {
		t.Error("shutdown accepted new job")
	}
}
