// ProjectForecast's local API gateway. Python owns project data and ML logic.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const maxBody = 2_000_000

type document struct {
	Name string `json:"name"`
	Text string `json:"text"`
}
type upload struct {
	ProjectID int        `json:"project_id"`
	Documents []document `json:"documents"`
}
type job struct {
	ID        string          `json:"id"`
	ProjectID int             `json:"project_id"`
	Status    string          `json:"status"`
	Created   time.Time       `json:"created_at"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	payload   []byte
}

type gateway struct {
	upstream string
	token    string
	client   *http.Client
	page     []byte
	queue    chan *job
	slots    chan struct{} // Bounds admitted jobs, including running jobs.
	apiSlots chan struct{}
	mu       sync.Mutex
	jobs     map[string]*job
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	workers  int
	timeout  time.Duration
	closed   bool
}

func newGateway(upstream, token string, page []byte, workers, capacity int, timeout time.Duration) *gateway {
	ctx, cancel := context.WithCancel(context.Background())
	g := &gateway{upstream: upstream, token: token, page: page, workers: workers, timeout: timeout, client: &http.Client{Timeout: timeout}, queue: make(chan *job, capacity), slots: make(chan struct{}, capacity), apiSlots: make(chan struct{}, 16), jobs: make(map[string]*job), ctx: ctx, cancel: cancel}
	for i := 0; i < workers; i++ {
		g.wg.Add(1)
		go g.worker()
	}
	return g
}
func (g *gateway) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.cancel()
	g.wg.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, j := range g.jobs {
		if j.Status == "queued" {
			j.Status = "cancelled"
			j.payload = nil
		}
	}
	g.client.CloseIdleConnections()
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}
func (g *gateway) call(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	select {
	case g.apiSlots <- struct{}{}:
		defer func() { <-g.apiSlots }()
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	// Retry only reads: replaying a project/time-entry write could duplicate data.
	attempts := 1
	if method == http.MethodGet {
		attempts = 3
	}
	var last error
	for i := 0; i < attempts; i++ {
		req, err := http.NewRequestWithContext(ctx, method, g.upstream+path, bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Internal-Token", g.token)
		res, err := g.client.Do(req)
		if err == nil {
			b, e := io.ReadAll(io.LimitReader(res.Body, 8_000_001))
			res.Body.Close()
			if e == nil && len(b) <= 8_000_000 {
				if res.StatusCode < 500 || i == attempts-1 {
					return res.StatusCode, b, nil
				}
				last = fmt.Errorf("service returned %d", res.StatusCode)
			} else {
				last = errors.New("invalid service response")
			}
		} else {
			last = err
		}
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(time.Duration(i+1) * 100 * time.Millisecond):
			}
		}
	}
	return 0, nil, last
}
func (g *gateway) worker() {
	defer g.wg.Done()
	for {
		select {
		case <-g.ctx.Done():
			return
		case j := <-g.queue:
			g.mu.Lock()
			if j.Status != "queued" || g.closed {
				j.payload = nil
				g.mu.Unlock()
				<-g.slots
				continue
			}
			j.Status = "running"
			payload := j.payload
			j.payload = nil
			g.mu.Unlock()
			ctx, cancel := context.WithTimeout(g.ctx, g.timeout)
			status, body, err := g.call(ctx, "POST", "/api/ingest", payload)
			cancel()
			g.mu.Lock()
			if err != nil {
				j.Status = "unknown"
				j.Error = "Processing connection ended; data may have committed. Check project evidence before resubmitting."
			} else if status >= 200 && status < 300 {
				j.Status = "succeeded"
				j.Result = json.RawMessage(body)
			} else {
				j.Status = "failed"
				j.Error = "Document processing failed"
				var detail map[string]any
				if json.Unmarshal(body, &detail) == nil {
					if s, ok := detail["error"].(string); ok {
						j.Error = s
					}
				}
			}
			g.mu.Unlock()
			<-g.slots
		}
	}
}
func (g *gateway) submit(w http.ResponseWriter, r *http.Request, body []byte) {
	var in upload
	if json.Unmarshal(body, &in) != nil || in.ProjectID <= 0 || len(in.Documents) < 1 || len(in.Documents) > 20 {
		fail(w, 400, "Provide a project_id and 1–20 documents")
		return
	}
	for _, d := range in.Documents {
		if strings.TrimSpace(d.Name) == "" || strings.TrimSpace(d.Text) == "" {
			fail(w, 400, "Each document needs a name and nonempty text")
			return
		}
	}
	// Validate the project before accepting a background job.
	status, state, err := g.call(r.Context(), "GET", "/api/state", nil)
	if err != nil || status != 200 {
		fail(w, 503, "Python service unavailable")
		return
	}
	var projects struct {
		Projects []struct {
			ID int `json:"id"`
		} `json:"projects"`
	}
	if json.Unmarshal(state, &projects) != nil {
		fail(w, 502, "Invalid project response")
		return
	}
	found := false
	for _, p := range projects.Projects {
		if p.ID == in.ProjectID {
			found = true
		}
	}
	if !found {
		fail(w, 404, "Project not found")
		return
	}
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		fail(w, 500, "Cannot create job ID")
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		fail(w, 503, "Server is stopping")
		return
	}
	select {
	case g.slots <- struct{}{}:
	default:
		w.Header().Set("Retry-After", "2")
		fail(w, 429, "Ingestion queue is full; retry after a job finishes")
		return
	}
	// Bound terminal status retention as well as queued payload memory.
	if len(g.jobs) >= 1000 {
		var oldest *job
		for _, j := range g.jobs {
			if j.Status != "queued" && j.Status != "running" && (oldest == nil || j.Created.Before(oldest.Created)) {
				oldest = j
			}
		}
		if oldest != nil {
			delete(g.jobs, oldest.ID)
		}
	}
	j := &job{ID: hex.EncodeToString(id), ProjectID: in.ProjectID, Status: "queued", Created: time.Now().UTC(), payload: body}
	g.jobs[j.ID] = j
	g.queue <- j
	w.Header().Set("Location", "/api/jobs/"+j.ID)
	respond(w, 202, j)
}
func (g *gateway) jobStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	g.mu.Lock()
	defer g.mu.Unlock()
	j, ok := g.jobs[id]
	if !ok {
		fail(w, 404, "Job not found or expired")
		return
	}
	switch r.Method {
	case "GET":
		respond(w, 200, j)
	case "DELETE":
		if j.Status == "queued" {
			j.Status = "cancelled"
			j.payload = nil
			respond(w, 200, j)
			return
		}
		if j.Status == "cancelled" {
			respond(w, 200, j)
			return
		}
		fail(w, 409, "Only queued jobs can be cancelled; running writes may already be committing")
	default:
		w.Header().Set("Allow", "GET, DELETE")
		fail(w, 405, "Method not allowed")
	}
}
func localHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if !localHost(r.Host) {
		fail(w, 403, "Local host required")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host {
		fail(w, 403, "Origin rejected")
		return
	}
	if r.URL.Path == "/" {
		if r.Method != "GET" {
			fail(w, 405, "Method not allowed")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(g.page)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/jobs/") {
		g.jobStatus(w, r)
		return
	}
	if r.URL.Path == "/api/health" {
		status, _, err := g.call(r.Context(), "GET", "/health", nil)
		if err != nil || status != 200 {
			fail(w, 503, "Python service unavailable")
			return
		}
		respond(w, 200, map[string]any{"status": "ok", "backend": "go", "workers": g.workers, "capacity": cap(g.slots)})
		return
	}
	read := r.URL.Path == "/api/state"
	writes := map[string]bool{"/api/projects": true, "/api/demo": true, "/api/time": true, "/api/progress": true, "/api/search": true, "/api/change": true, "/api/approve": true, "/api/ingest": true}
	if !read && !writes[r.URL.Path] {
		fail(w, 404, "Not found")
		return
	}
	method := "POST"
	if read {
		method = "GET"
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		fail(w, 405, "Method not allowed")
		return
	}
	var body []byte
	if method == "POST" {
		var err error
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		body, err = io.ReadAll(r.Body)
		if err != nil {
			fail(w, 413, "Request must be under 2 MB")
			return
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(body, &obj) != nil || obj == nil {
			fail(w, 400, "Expected a JSON object")
			return
		}
	}
	if r.URL.Path == "/api/ingest" {
		g.submit(w, r, body)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.timeout)
	defer cancel()
	status, result, err := g.call(ctx, method, r.URL.Path, body)
	if err != nil {
		fail(w, 502, "Python service unavailable or timed out; writes are not automatically retried")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(result)
}
func integer(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, e := strconv.Atoi(v)
	if e != nil || n < 1 || n > 256 {
		log.Fatalf("%s must be between 1 and 256", name)
	}
	return n
}
func main() {
	upstream := os.Getenv("PYTHON_SERVICE_URL")
	if upstream == "" {
		upstream = "http://127.0.0.1:8766"
	}
	u, err := url.Parse(upstream)
	if err != nil || u.Scheme != "http" || !localHost(u.Host) || u.Path != "" || u.RawQuery != "" || u.User != nil {
		log.Fatal("PYTHON_SERVICE_URL must be a loopback HTTP origin")
	}
	token := os.Getenv("FORECAST_INTERNAL_TOKEN")
	if token == "" {
		log.Fatal("FORECAST_INTERNAL_TOKEN is required; use python3 run.py")
	}
	page, err := os.ReadFile("index.html")
	if err != nil {
		log.Fatal(err)
	}
	g := newGateway(upstream, token, page, integer("INGEST_WORKERS", 2), integer("INGEST_CAPACITY", 8), 60*time.Second)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8765"
	}
	if n, e := strconv.Atoi(port); e != nil || n < 1 || n > 65535 {
		log.Fatal("Invalid PORT")
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: g, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 * 1024}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		<-signals
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
		g.close()
		close(done)
	}()
	log.Printf("Go backend ready at http://%s", server.Addr)
	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		g.close()
		log.Fatal(err)
	}
	<-done
}
