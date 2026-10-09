//go:build !windows

package ledger

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

func dirOf(path string) string {
	return filepath.Dir(path)
}

func fsyncFile(f *os.File) error {
	return f.Sync()
}

// syncDir flushes directory entry changes. A few filesystems return errors
// that indicate the operation is unsupported; these are ignored, matching the
// behavior of the local backend.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = d.Sync()
	if err != nil &&
		(errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.ENOENT) ||
			errors.Is(err, syscall.EINVAL) || isMacENOTTY(err)) {
		err = nil
	}

	cerr := d.Close()
	if err == nil {
		err = cerr
	}
	return err
}

func isMacENOTTY(err error) bool {
	return runtime.GOOS == "darwin" && errors.Is(err, syscall.ENOTTY)
}
