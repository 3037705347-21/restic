//go:build !windows

package ledger

import (
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking advisory lock on f. It returns
// errLockBusy when another process already holds the lock.
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return errLockBusy
	}
	return err
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
