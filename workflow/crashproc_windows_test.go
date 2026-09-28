//go:build windows

package workflow

import (
	"os/exec"
	"syscall"
)

func setpgid() *syscall.SysProcAttr { return nil }

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
