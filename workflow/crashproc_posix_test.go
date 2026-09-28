//go:build !windows

package workflow

import (
	"os/exec"
	"syscall"
)

func setpgid() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	// Negative pid kills the whole group, including the blocking test binary.
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
