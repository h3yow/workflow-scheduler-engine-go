package workflow

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestCrashProcess is executed in a subprocess. It registers a workflow,
// starts a run, prints "RUN <id>" once task "b" is RUNNING and then blocks
// forever; the parent kills -9 the whole process group, leaving exactly the
// on-disk intermediate state a real crash produces.
func TestCrashProcess(t *testing.T) {
	if os.Getenv("WF_CRASH_MODE") == "" {
		return
	}
	dir := os.Getenv("WF_STORE_DIR")
	reg := NewRegistry()
	reg.Register("a", func(ctx context.Context, tc TaskContext) (string, error) { return "a", nil })
	reg.Register("b", func(ctx context.Context, tc TaskContext) (string, error) {
		<-ctx.Done() // keeps the process alive while running
		return "", ctx.Err()
	})
	reg.Register("c", func(ctx context.Context, tc TaskContext) (string, error) { return "c", nil })
	st, err := NewFileStore(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "store:", err)
		os.Exit(2)
	}
	eng, err := NewEngine(st, reg, WithRecovery(RecoveryResume))
	if err != nil {
		fmt.Fprintln(os.Stderr, "engine:", err)
		os.Exit(2)
	}
	def := WorkflowDef{
		Name: "recover",
		Tasks: []TaskDef{
			{ID: "a"},
			{ID: "b", Dependencies: []string{"a"}},
			{ID: "c", Dependencies: []string{"b"}},
		},
		TriggerConcurrency: ConcurrencyReject,
	}
	if err := eng.Register(def); err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		os.Exit(2)
	}
	run, err := eng.Trigger(context.Background(), "recover", TriggerManual)
	if err != nil {
		fmt.Fprintln(os.Stderr, "trigger:", err)
		os.Exit(2)
	}
	for {
		bt, err := st.GetTaskByRef(run.ID, "b")
		if err == nil && bt.State == TaskRunning {
			fmt.Printf("RUN %s\n", run.ID)
			os.Stdout.Sync()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {} // block until SIGKILL
}

// crashInSubprocess starts the current test binary as a child in its own
// process group, waits for "RUN <id>", then SIGKILLs the group.
func crashInSubprocess(t *testing.T, dir string) RunID {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run", "TestCrashProcess", "-test.v")
	cmd.Env = append(os.Environ(),
		"WF_CRASH_MODE=1",
		"WF_STORE_DIR="+dir,
	)
	cmd.SysProcAttr = setpgid()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Drain stderr on a goroutine; it is not parsed, so no shared buffer.
	go func() { _, _ = io.Copy(io.Discard, stderr) }()

	var runID RunID
	sc := bufio.NewScanner(stdout)
	deadline := time.Now().Add(10 * time.Second)
	for runID == "" && time.Now().Before(deadline) {
		if !sc.Scan() {
			if err := sc.Err(); err != nil {
				_ = killProcessGroup(cmd)
				t.Fatalf("scan child stdout: %v", err)
			}
			break
		}
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "RUN ") {
			runID = RunID(strings.TrimSpace(strings.TrimPrefix(line, "RUN ")))
		}
	}
	if runID == "" {
		_ = killProcessGroup(cmd)
		t.Fatal("child never reported RUN")
	}
	if err := killProcessGroup(cmd); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	return runID
}
