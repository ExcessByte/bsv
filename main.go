package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	appName       = "bsv"
	stopGrace     = 5 * time.Second
	killWait      = 2 * time.Second
	readTimeout   = 5 * time.Second
	handleTimeout = 30 * time.Second
	clientTimeout = 30 * time.Second
	maxLogSize    = 1 << 20
)

var (
	ErrNotRunning = errors.New("not running")
	errHelp       = errors.New("help requested")
)

func validName(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, `/\`)
}

type Request struct {
	Action string `json:"action"`
	Name   string `json:"name,omitempty"`
}

type ServiceStatus struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Running     bool   `json:"running"`
	Enabled     bool   `json:"enabled"`
	PID         int    `json:"pid,omitempty"`
	UptimeSec   int64  `json:"uptime_sec,omitempty"`
	LastExit    string `json:"last_exit,omitempty"`
}

type Response struct {
	Success  bool            `json:"success"`
	Error    string          `json:"error,omitempty"`
	Message  string          `json:"message,omitempty"`
	Services []ServiceStatus `json:"services,omitempty"`
}

func fail(format string, a ...any) Response {
	return Response{Error: fmt.Sprintf(format, a...)}
}

func socketDir() string {
	if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
		return filepath.Join(x, appName)
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d", appName, os.Getuid()))
}

func socketPath() string { return filepath.Join(socketDir(), appName+".sock") }

func defaultDataDir() string {
	if d := os.Getenv("BSV_DIR"); d != "" {
		return d
	}
	if d, err := os.UserConfigDir(); err == nil {
		return filepath.Join(d, appName)
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, "."+appName)
	}
	return "." + appName
}

func ensureSocketDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
		return fmt.Errorf("%s is owned by uid %d, refusing to use it", dir, st.Uid)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return os.Chmod(dir, 0o700)
	}
	return nil
}

type ServiceConfig struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Cmd         string   `json:"cmd"`
	Args        []string `json:"args"`
}

type service struct {
	cfg ServiceConfig
	mu  sync.Mutex
}

func LoadServiceConfigs(dir string) map[string]*service {
	out := make(map[string]*service)
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("cannot read services dir %s: %v", dir, err)
		return out
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("skipping %s: %v", path, err)
			continue
		}
		var cfg ServiceConfig
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("skipping %s: invalid JSON: %v", path, err)
			continue
		}
		switch {
		case !validName(cfg.Name):
			log.Printf("skipping %s: invalid name %q", path, cfg.Name)
			continue
		case cfg.Cmd == "":
			log.Printf("skipping %s: empty cmd", path)
			continue
		}
		if _, dup := out[cfg.Name]; dup {
			log.Printf("skipping %s: duplicate service name %q", path, cfg.Name)
			continue
		}
		out[cfg.Name] = &service{cfg: cfg}
	}
	return out
}

type ServiceState struct {
	Enabled bool `json:"enabled"`
}

type stateFile struct {
	Services map[string]ServiceState `json:"services"`
}

type State struct {
	mu       sync.Mutex
	path     string
	services map[string]ServiceState
}

func LoadState(path string) *State {
	s := &State{path: path, services: make(map[string]ServiceState)}
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("cannot read state %s: %v", path, err)
		}
		return s
	}
	var f stateFile
	if err := json.Unmarshal(data, &f); err != nil {
		log.Printf("state file %s is corrupt, starting fresh: %v", path, err)
		return s
	}
	if f.Services != nil {
		s.services = f.Services
	}
	return s
}

func (s *State) Enabled(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.services[name]
	return !ok || st.Enabled
}

func (s *State) SetEnabled(name string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.services[name]; ok && st.Enabled == enabled {
		return nil
	}
	s.services[name] = ServiceState{Enabled: enabled}
	return s.saveLocked()
}

func (s *State) saveLocked() error {
	data, err := json.Marshal(stateFile{Services: s.services})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), "state-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}

type job struct {
	name    string
	cmd     *exec.Cmd
	pid     int
	started time.Time
	done    chan struct{}
}

type exitInfo struct {
	at   time.Time
	desc string
}

type JobManager struct {
	mu     sync.Mutex
	jobs   map[string]*job
	last   map[string]exitInfo
	logDir string
}

func NewJobManager(logDir string) *JobManager {
	return &JobManager{
		jobs:   make(map[string]*job),
		last:   make(map[string]exitInfo),
		logDir: logDir,
	}
}

func openLog(dir, name string) (*os.File, error) {
	path := filepath.Join(dir, name+".log")
	if fi, err := os.Stat(path); err == nil && fi.Size() > maxLogSize {
		_ = os.Rename(path, path+".1")
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
}

func (jm *JobManager) Start(cfg ServiceConfig) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if _, running := jm.jobs[cfg.Name]; running {
		return errors.New("already running")
	}

	logFile, err := openLog(jm.logDir, cfg.Name)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}

	cmd := exec.Command(cfg.Cmd, cfg.Args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start %q: %w", cfg.Cmd, err)
	}

	j := &job{
		name:    cfg.Name,
		cmd:     cmd,
		pid:     cmd.Process.Pid,
		started: time.Now(),
		done:    make(chan struct{}),
	}
	jm.jobs[cfg.Name] = j
	log.Printf("service %q started (pid %d)", cfg.Name, j.pid)

	go jm.reap(j, logFile)
	return nil
}

func (jm *JobManager) reap(j *job, logFile *os.File) {
	err := j.cmd.Wait()
	logFile.Close()

	desc := "exit status 0"
	if err != nil {
		desc = err.Error()
	}

	jm.mu.Lock()
	if jm.jobs[j.name] == j {
		delete(jm.jobs, j.name)
	}
	jm.last[j.name] = exitInfo{at: time.Now(), desc: desc}
	jm.mu.Unlock()

	close(j.done)
	log.Printf("service %q (pid %d) exited: %s", j.name, j.pid, desc)
}

func (jm *JobManager) Stop(name string) error {
	jm.mu.Lock()
	j := jm.jobs[name]
	jm.mu.Unlock()
	if j == nil {
		return ErrNotRunning
	}
	return j.terminate()
}

func (j *job) waitDone(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-j.done:
		return true
	case <-t.C:
		return false
	}
}

func (j *job) terminate() error {
	signalGroup := func(sig syscall.Signal) {
		select {
		case <-j.done:
		default:
			_ = syscall.Kill(-j.pid, sig)
		}
	}

	signalGroup(syscall.SIGTERM)
	if j.waitDone(stopGrace) {
		return nil
	}

	log.Printf("service %q ignored SIGTERM for %s, sending SIGKILL", j.name, stopGrace)
	signalGroup(syscall.SIGKILL)
	if j.waitDone(killWait) {
		return nil
	}
	return errors.New("process still alive after SIGKILL")
}

func (jm *JobManager) StopAll() {
	jm.mu.Lock()
	snapshot := make([]*job, 0, len(jm.jobs))
	for _, j := range jm.jobs {
		snapshot = append(snapshot, j)
	}
	jm.mu.Unlock()

	var wg sync.WaitGroup
	for _, j := range snapshot {
		wg.Add(1)
		go func(j *job) {
			defer wg.Done()
			if err := j.terminate(); err != nil {
				log.Printf("service %q: %v", j.name, err)
			}
		}(j)
	}
	wg.Wait()
}

func (jm *JobManager) Snapshot(name string) (running bool, pid int, uptime time.Duration, last string) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if j, ok := jm.jobs[name]; ok {
		return true, j.pid, time.Since(j.started), ""
	}
	if e, ok := jm.last[name]; ok {
		last = fmt.Sprintf("%s (%s ago)", e.desc, time.Since(e.at).Round(time.Second))
	}
	return false, 0, 0, last
}

type Server struct {
	socket   string
	services map[string]*service
	jobs     *JobManager
	state    *State
	wg       sync.WaitGroup
}

func runDaemon(dir string) error {
	logDir := filepath.Join(dir, "logs")
	svcDir := filepath.Join(dir, "services")
	for _, d := range []string{logDir, svcDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}

	srv := &Server{
		socket:   socketPath(),
		services: LoadServiceConfigs(svcDir),
		jobs:     NewJobManager(logDir),
		state:    LoadState(filepath.Join(dir, "state.json")),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return srv.Run(ctx)
}

func listenUnix(path string) (net.Listener, error) {
	if err := ensureSocketDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if c, err := net.DialTimeout("unix", path, time.Second); err == nil {
		c.Close()
		return nil, fmt.Errorf("another daemon is already listening on %s", path)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := listenUnix(s.socket)
	if err != nil {
		return err
	}
	log.Printf("listening on %s (%d services configured)", s.socket, len(s.services))

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	s.startEnabled()
	debug.FreeOSMemory()

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go s.handle(c)
	}

	log.Printf("shutting down")
	s.wg.Wait()
	s.jobs.StopAll()
	log.Printf("bye")
	return nil
}

func (s *Server) startEnabled() {
	for _, name := range s.sortedNames() {
		if !s.state.Enabled(name) {
			log.Printf("service %q is disabled, not starting", name)
			continue
		}
		if err := s.jobs.Start(s.services[name].cfg); err != nil {
			log.Printf("service %q failed to start: %v", name, err)
		}
	}
}

func (s *Server) sortedNames() []string {
	names := make([]string, 0, len(s.services))
	for n := range s.services {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer c.Close()

	_ = c.SetReadDeadline(time.Now().Add(readTimeout))
	_ = c.SetWriteDeadline(time.Now().Add(handleTimeout))

	var req Request
	var resp Response
	if err := json.NewDecoder(c).Decode(&req); err != nil {
		resp = fail("bad request: %v", err)
	} else {
		resp = s.dispatch(req)
	}
	if err := json.NewEncoder(c).Encode(resp); err != nil {
		log.Printf("write response: %v", err)
	}
}

func (s *Server) dispatch(req Request) Response {
	if req.Action == "status" {
		return s.status(req.Name)
	}
	svc, ok := s.services[req.Name]
	if !ok {
		return fail("unknown service %q", req.Name)
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()

	switch req.Action {
	case "start":
		return s.doStart(svc)
	case "stop":
		return s.doStop(svc)
	case "restart":
		return s.doRestart(svc)
	default:
		return fail("invalid action %q", req.Action)
	}
}

func (s *Server) persist(name string, enabled bool) {
	if err := s.state.SetEnabled(name, enabled); err != nil {
		log.Printf("could not persist state for %q: %v", name, err)
	}
}

func (s *Server) doStart(svc *service) Response {
	if err := s.jobs.Start(svc.cfg); err != nil {
		return fail("%v", err)
	}
	s.persist(svc.cfg.Name, true)
	return Response{Success: true}
}

func (s *Server) doStop(svc *service) Response {
	name := svc.cfg.Name
	s.persist(name, false)
	err := s.jobs.Stop(name)
	switch {
	case errors.Is(err, ErrNotRunning):
		return Response{Success: true, Message: "was not running; marked disabled"}
	case err != nil:
		return fail("%v", err)
	}
	return Response{Success: true}
}

func (s *Server) doRestart(svc *service) Response {
	name := svc.cfg.Name
	if err := s.jobs.Stop(name); err != nil && !errors.Is(err, ErrNotRunning) {
		return fail("stop: %v", err)
	}
	if err := s.jobs.Start(svc.cfg); err != nil {
		return fail("start: %v", err)
	}
	s.persist(name, true)
	return Response{Success: true}
}

func (s *Server) status(name string) Response {
	names := s.sortedNames()
	if name != "" {
		if _, ok := s.services[name]; !ok {
			return fail("unknown service %q", name)
		}
		names = []string{name}
	}
	out := make([]ServiceStatus, 0, len(names))
	for _, n := range names {
		running, pid, up, last := s.jobs.Snapshot(n)
		out = append(out, ServiceStatus{
			Name:        n,
			Description: s.services[n].cfg.Description,
			Running:     running,
			Enabled:     s.state.Enabled(n),
			PID:         pid,
			UptimeSec:   int64(up.Seconds()),
			LastExit:    last,
		})
	}
	return Response{Success: true, Services: out}
}

func sendRequest(req Request) (*Response, error) {
	sock := socketPath()
	conn, err := net.DialTimeout("unix", sock, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("cannot reach daemon at %s (is it running? try '%s daemon'): %w",
			sock, filepath.Base(os.Args[0]), err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(clientTimeout))

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return &resp, nil
}

func printStatus(w *os.File, list []ServiceStatus) {
	if len(list) == 0 {
		fmt.Fprintln(w, "no services configured")
		return
	}
	rows := make([][]string, 0, len(list)+1)
	rows = append(rows, []string{"NAME", "STATE", "PID", "UPTIME", "ENABLED", "LAST EXIT", "DESCRIPTION"})
	for _, s := range list {
		state, pid, up := "stopped", "-", "-"
		if s.Running {
			state = "running"
			pid = fmt.Sprint(s.PID)
			up = (time.Duration(s.UptimeSec) * time.Second).String()
		}
		enabled := "no"
		if s.Enabled {
			enabled = "yes"
		}
		last := s.LastExit
		if last == "" {
			last = "-"
		}
		rows = append(rows, []string{s.Name, state, pid, up, enabled, last, s.Description})
	}

	var width [7]int
	for _, r := range rows {
		for i, c := range r {
			if len(c) > width[i] {
				width[i] = len(c)
			}
		}
	}

	var b strings.Builder
	for _, r := range rows {
		for i, c := range r {
			b.WriteString(c)
			if i < len(r)-1 {
				for n := len(c); n < width[i]+2; n++ {
					b.WriteByte(' ')
				}
			}
		}
		b.WriteByte('\n')
	}
	_, _ = w.WriteString(b.String())
}

func usage(w *os.File) {
	prog := filepath.Base(os.Args[0])
	fmt.Fprintf(w, `Usage: %[1]s <command> [arguments]

Commands:
  daemon [--dir DIR]   Run the supervisor in the foreground
  start   <name>       Start a service (and re-enable it)
  stop    <name>       Stop a service (and keep it off across daemon restarts)
  restart <name>       Stop then start a service
  status  [name]       Show all services, or just one
  help                 Show this message

Data directory: %[2]s  (override with --dir or $BSV_DIR)
Socket:         %[3]s
`, prog, defaultDataDir(), socketPath())
}

func parseDaemonArgs(args []string) (dir string, err error) {
	dir = defaultDataDir()
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-h" || a == "--help":
			return "", errHelp
		case a == "--dir":
			if i+1 >= len(args) {
				return "", errors.New("--dir needs a value")
			}
			i++
			dir = args[i]
		case strings.HasPrefix(a, "--dir="):
			dir = a[len("--dir="):]
		default:
			return "", fmt.Errorf("unknown option %q", a)
		}
	}
	return dir, nil
}

func run(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}
	command, rest := args[0], args[1:]

	switch command {
	case "daemon":
		dir, err := parseDaemonArgs(rest)
		if errors.Is(err, errHelp) {
			usage(os.Stdout)
			return 0
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "daemon: %v\n", err)
			return 2
		}
		if err := runDaemon(dir); err != nil {
			fmt.Fprintf(os.Stderr, "daemon: %v\n", err)
			return 1
		}
		return 0

	case "start", "stop", "restart", "status":
		name := ""
		if len(rest) > 0 {
			name = rest[0]
		}
		if name == "" && command != "status" {
			fmt.Fprintf(os.Stderr, "Error: '%s' needs a service name\n", command)
			return 2
		}
		resp, err := sendRequest(Request{Action: command, Name: name})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		if !resp.Success {
			fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
			return 1
		}
		if command == "status" {
			printStatus(os.Stdout, resp.Services)
		} else if resp.Message != "" {
			fmt.Printf("%s %s: %s\n", command, name, resp.Message)
		} else {
			fmt.Printf("Success: %s %s\n", command, name)
		}
		return 0

	case "help", "-h", "--help":
		usage(os.Stdout)
		return 0

	default:
		fmt.Fprintf(os.Stderr, "Sorry, but '%s' is invalid. Type 'help' to get a list.\n", command)
		return 2
	}
}

func main() {
	runtime.GOMAXPROCS(1)
	debug.SetGCPercent(20)
	os.Exit(run(os.Args[1:]))
}
