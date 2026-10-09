//go:build windows

package ledger

import (
	"os"
	"path/filepath"
)

func dirOf(path string) string {
	return filepath.Dir(path)
}

func fsyncFile(f *os.File) error {
	return f.Sync()
}

// Windows has no directory fsync; the file fsync before the rename and the
// atomic MoveFileEx performed by os.Rename provide the durability guarantees
// the ledger relies on.
func syncDir(_ string) error {
	return nil
}
