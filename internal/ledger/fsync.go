package ledger

import (
	"io"
	"os"
)

// writeFileAtomic writes data to path via a temp file in the same directory:
// temp write + fsync, rename into place, fsync of the directory. The 0600
// permission bits are set before the rename.
func writeFileAtomic(path string, data []byte) error {
	return writeFileAtomicFn(path, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	})
}

// writeFileAtomicFn streams content to path with the same crash-safety
// guarantees as writeFileAtomic.
func writeFileAtomicFn(path string, write func(io.Writer) error) (err error) {
	dir := dirOf(path)

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if err = os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	if err = write(tmp); err != nil {
		return err
	}
	if err = fsyncFile(tmp); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncDir(dir)
}
