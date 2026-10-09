package restic

// FileType is the type of a file in the repository.
// Numeric values must match backend.FileType; enforced in internal/repository/filetype.go.
type FileType uint8

// These are the different data types a backend can store.
const (
	PackFile FileType = 1 + iota
	KeyFile
	LockFile
	SnapshotFile
	IndexFile
	ConfigFile
	// CopyStateFile stores per-snapshot progress of the `copy` command. New file
	// types must be appended to keep the numeric values of existing types stable.
	CopyStateFile
)

// Keep in sync with backend.FileType.String().
func (t FileType) String() string {
	s := "invalid"
	switch t {
	case PackFile:
		// Spelled "data" instead of "pack" for historical reasons.
		s = "data"
	case KeyFile:
		s = "key"
	case LockFile:
		s = "lock"
	case SnapshotFile:
		s = "snapshot"
	case IndexFile:
		s = "index"
	case ConfigFile:
		s = "config"
	case CopyStateFile:
		s = "copy-state"
	}
	return s
}

// WriteableFileType defines the different data types that can be modified via SaveUnpacked or RemoveUnpacked.
type WriteableFileType FileType

const (
	// WriteableSnapshotFile is the WriteableFileType for snapshots.
	WriteableSnapshotFile = WriteableFileType(SnapshotFile)
	// WriteableCopyStateFile is the WriteableFileType for `copy` progress records.
	WriteableCopyStateFile = WriteableFileType(CopyStateFile)
)

func (w *WriteableFileType) ToFileType() FileType {
	switch *w {
	case WriteableSnapshotFile:
		return SnapshotFile
	case WriteableCopyStateFile:
		return CopyStateFile
	default:
		panic("invalid WriteableFileType")
	}
}

type FileTypes interface {
	FileType | WriteableFileType
}
