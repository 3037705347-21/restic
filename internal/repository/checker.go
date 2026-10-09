package repository

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/repository/hashing"
	"github.com/restic/restic/internal/repository/index"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

const maxStreamBufferSize = 4 * 1024 * 1024

// ErrIncompletePackEntry is returned when indexes contain different data for a pack.
type ErrIncompletePackEntry struct {
	PackID  restic.ID
	Indexes restic.IDSet
}

func (e *ErrIncompletePackEntry) Error() string {
	return fmt.Sprintf("pack %v has different data in indexes: %v", e.PackID, e.Indexes)
}

// ErrDuplicatePacks is returned when a pack is found in more than one index.
type ErrDuplicatePacks struct {
	PackID  restic.ID
	Indexes restic.IDSet
}

func (e *ErrDuplicatePacks) Error() string {
	return fmt.Sprintf("pack %v contained in several indexes: %v", e.PackID, e.Indexes)
}

// ErrMixedPack is returned when a pack is found that contains both tree and data blobs.
type ErrMixedPack struct {
	PackID restic.ID
}

func (e *ErrMixedPack) Error() string {
	return fmt.Sprintf("pack %v contains a mix of tree and data blobs", e.PackID.Str())
}

// ErrPackMetadata describes an error with a specific pack. It is used for missing, truncated or orphaned packs.
// Errors of the actual pack data are returned as ErrPackData.
type ErrPackMetadata struct {
	ID        restic.ID
	Orphaned  bool
	Truncated bool
	Missing   bool
	Err       error
}

func (e *ErrPackMetadata) Error() string {
	return "pack " + e.ID.String() + ": " + e.Err.Error()
}

// ErrPackData is returned if errors are discovered while verifying a packfile
type ErrPackData struct {
	PackID restic.ID
	errs   []error
}

func (e *ErrPackData) Error() string {
	return fmt.Sprintf("pack %v contains %v errors: %v", e.PackID, len(e.errs), e.errs)
}

// PackCheckResult is the outcome of verifying a single pack. GoodBlobs lists
// the blob handles that were cryptographically verified while reading a
// damaged pack; it is nil for undamaged packs and for packs that could not be
// downloaded at all.
type PackCheckResult struct {
	ID        restic.ID
	GoodBlobs []restic.BlobHandle
	Err       error
}

// Checker handles index-related operations for repository checking.
type Checker struct {
	repo *Repository
}

// newChecker creates a new Checker.
func newChecker(repo *Repository) *Checker {
	return &Checker{
		repo: repo,
	}
}
func computePackTypes(ctx context.Context, idx restic.ListBlobser) (map[restic.ID]restic.BlobType, error) {
	packs := make(map[restic.ID]restic.BlobType)
	err := idx.ListBlobs(ctx, func(pb restic.PackBlob) {
		packID := pb.PackID()
		h := pb.Handle()
		tpe, exists := packs[packID]
		if exists {
			if h.Type != tpe {
				tpe = restic.InvalidBlob
			}
		} else {
			tpe = h.Type
		}
		packs[packID] = tpe
	})
	return packs, err
}

// LoadIndex loads all index files.
func (c *Checker) LoadIndex(ctx context.Context, p restic.TerminalCounterFactory) (hints []error, errs []error) {
	debug.Log("Start")
	packToIndex := make(map[restic.ID]restic.IDSet)
	// in restic < 0.10.0, the blobs of a pack could be split over multiple indexes.
	// by now this is considered as repository damage.
	packToPackBlobHash := make(map[restic.ID]restic.IDSet)

	// Use the repository's internal loadIndexWithCallback to handle per-index errors
	err := c.repo.loadIndexWithCallback(ctx, p, func(id restic.ID, idx *index.Index, err error) error {
		debug.Log("process index %v, err %v", id, err)
		err = errors.Wrapf(err, "error loading index %v", id)

		if err != nil {
			errs = append(errs, err)
			return nil
		}

		debug.Log("process blobs")
		cnt := 0
		for blob := range idx.Values() {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cnt++

			packID := blob.PackID()
			if _, ok := packToIndex[packID]; !ok {
				packToIndex[packID] = restic.NewIDSet()
			}
			packToIndex[packID].Insert(id)
		}

		for pbs := range idx.EachByPack(ctx, restic.NewIDSet()) {
			packBlobHash := index.PackBlobsHash(pbs)
			if _, ok := packToPackBlobHash[pbs.PackID]; !ok {
				packToPackBlobHash[pbs.PackID] = restic.NewIDSet()
			}
			packToPackBlobHash[pbs.PackID].Insert(packBlobHash)
		}

		debug.Log("%d blobs processed", cnt)
		return nil
	})
	if err != nil {
		// failed to load the index
		return hints, append(errs, err)
	}

	packTypes, err := computePackTypes(ctx, c.repo)
	if err != nil {
		return hints, append(errs, err)
	}

	debug.Log("checking for duplicate packs")
	for packID := range packTypes {
		debug.Log("  check pack %v: contained in %d indexes", packID, len(packToIndex[packID]))
		if len(packToPackBlobHash[packID]) > 1 {
			hints = append(hints, &ErrIncompletePackEntry{
				PackID:  packID,
				Indexes: packToIndex[packID],
			})
		} else if len(packToIndex[packID]) > 1 {
			hints = append(hints, &ErrDuplicatePacks{
				PackID:  packID,
				Indexes: packToIndex[packID],
			})
		}
		if packTypes[packID] == restic.InvalidBlob {
			hints = append(hints, &ErrMixedPack{
				PackID: packID,
			})
		}
	}

	return hints, errs
}

// PackFingerprint identifies the current index state of a single pack. A
// previously recorded verification must be redone when either value changes.
type PackFingerprint struct {
	// BlobHash is index.PackBlobsHash over the pack's index entries.
	BlobHash restic.ID
	// Size is the pack size as recorded in the index.
	Size int64
}

// PackFingerprints returns the current fingerprint of every pack referenced by
// the index.
func (c *Checker) PackFingerprints(ctx context.Context) (map[restic.ID]PackFingerprint, error) {
	sizes, err := pack.Size(ctx, c.repo, false)
	if err != nil {
		return nil, err
	}

	packSet := restic.NewIDSet()
	for id := range sizes {
		packSet.Insert(id)
	}

	out := make(map[restic.ID]PackFingerprint, len(sizes))
	for pbs := range c.repo.listPacksFromIndex(ctx, packSet) {
		out[pbs.PackID] = PackFingerprint{
			BlobHash: index.PackBlobsHash(pbs),
			Size:     sizes[pbs.PackID],
		}
	}
	return out, nil
}

// Packs checks that all packs referenced in the index are still available and
// there are no packs that aren't in an index. errChan is closed after all
// packs have been checked.
func (c *Checker) Packs(ctx context.Context, errChan chan<- error) {
	defer close(errChan)

	// compute pack size using index entries
	packs, err := pack.Size(ctx, c.repo, false)
	if err != nil {
		errChan <- err
		return
	}

	debug.Log("checking for %d packs", len(packs))

	debug.Log("listing repository packs")
	repoPacks := make(map[restic.ID]int64)

	err = c.repo.List(ctx, restic.PackFile, func(id restic.ID, size int64) error {
		repoPacks[id] = size
		return nil
	})

	if err != nil {
		errChan <- err
	}

	for id, size := range packs {
		reposize, ok := repoPacks[id]
		// remove from repoPacks so we can find orphaned packs
		delete(repoPacks, id)

		// missing: present in c.packs but not in the repo
		if !ok {
			select {
			case <-ctx.Done():
				return
			case errChan <- &ErrPackMetadata{ID: id, Missing: true, Err: errors.New("does not exist")}:
			}
			continue
		}

		// size not matching: present in c.packs and in the repo, but sizes do not match
		if size != reposize {
			select {
			case <-ctx.Done():
				return
			case errChan <- &ErrPackMetadata{ID: id, Truncated: true, Err: errors.Errorf("unexpected file size: got %d, expected %d", reposize, size)}:
			}
		}
	}

	// orphaned: present in the repo but not in c.packs
	for orphanID := range repoPacks {
		select {
		case <-ctx.Done():
			return
		case errChan <- &ErrPackMetadata{ID: orphanID, Orphaned: true, Err: errors.New("not referenced in any index")}:
		}
	}
}

// ReadPacks loads data from specified packs and checks the integrity. The
// optional hook receives every pack verification result (including the blob
// handles that could still be verified for a damaged pack); a non-nil hook
// error aborts the read. The hook is called concurrently and must be safe for
// that.
func (c *Checker) ReadPacks(ctx context.Context, filter func(packs map[restic.ID]int64) map[restic.ID]int64, printer restic.Printer, errChan chan<- error, hook func(PackCheckResult) error) {
	defer close(errChan)

	// compute pack size using index entries
	packs, err := pack.Size(ctx, c.repo, false)
	if err != nil {
		errChan <- err
		return
	}
	packs = filter(packs)

	p := printer.NewCounter("packs")
	p.SetMax(uint64(len(packs)))
	defer p.Done()

	packSet := restic.NewIDSet()
	for pack := range packs {
		packSet.Insert(pack)
	}

	if feature.Flag.Enabled(feature.S3Restore) {
		job, err := c.repo.StartWarmup(ctx, packSet)
		if err != nil {
			errChan <- err
			return
		}
		if job.HandleCount() != 0 {
			printer.P("warming up %d packs from cold storage, this may take a while...", job.HandleCount())
			if err := job.Wait(ctx); err != nil {
				errChan <- err
				return
			}
		}
	}

	// gctx is cancelled when any worker returns an error; it must not shadow
	// the caller's ctx, which decides whether the failure is still reported.
	g, gctx := errgroup.WithContext(ctx)
	type checkTask struct {
		id    restic.ID
		size  int64
		blobs pack.Blobs
	}
	ch := make(chan checkTask)

	// as packs are streamed the concurrency is limited by IO
	workerCount := int(c.repo.Connections())
	// run workers
	for range workerCount {
		g.Go(func() error {
			bufRd := bufio.NewReaderSize(nil, maxStreamBufferSize)
			dec, err := zstd.NewReader(nil)
			if err != nil {
				panic(err)
			}
			defer dec.Close()
			for {
				var ps checkTask
				var ok bool

				select {
				case <-gctx.Done():
					return nil
				case ps, ok = <-ch:
					if !ok {
						return nil
					}
				}

				goodBlobs, err := checkPack(gctx, c.repo, ps.id, ps.blobs, ps.size, bufRd, dec)
				p.Add(1)
				if hook != nil {
					if hookErr := hook(PackCheckResult{ID: ps.id, GoodBlobs: goodBlobs, Err: err}); hookErr != nil {
						return hookErr
					}
				}
				if err == nil {
					continue
				}

				select {
				case <-gctx.Done():
					return nil
				case errChan <- err:
				}
			}
		})
	}

	// push packs to ch
	for pbs := range c.repo.listPacksFromIndex(gctx, packSet) {
		size := packs[pbs.PackID]
		debug.Log("listed %v", pbs.PackID)
		select {
		case ch <- checkTask{id: pbs.PackID, size: size, blobs: pbs.Blobs}:
		case <-gctx.Done():
		}
	}
	close(ch)

	err = g.Wait()
	if err != nil && ctx.Err() == nil {
		// surface worker/hook errors unless the caller cancelled the run;
		// the errgroup's own context is always cancelled when a worker fails,
		// so checking it here would hide the originating error
		errChan <- err
	}
}

// checkPack reads a pack and checks the integrity of all blobs. On a damaged
// pack it also returns the handles of blobs that could be verified
// successfully in the final verification attempt.
func checkPack(ctx context.Context, r *Repository, id restic.ID, blobs pack.Blobs, size int64, bufRd *bufio.Reader, dec *zstd.Decoder) (goodBlobs []restic.BlobHandle, err error) {
	goodBlobs, err = checkPackInner(ctx, r, id, blobs, size, bufRd, dec)
	if err != nil {
		if r.cache != nil {
			// ignore error as there's not much we can do here
			_ = r.cache.Forget(backend.Handle{Type: backend.PackFile, Name: id.String()})
		}

		// retry pack verification to detect transient errors
		goodBlobs2, err2 := checkPackInner(ctx, r, id, blobs, size, bufRd, dec)
		if err2 != nil {
			return goodBlobs2, err2
		}
		return nil, fmt.Errorf("check successful on second attempt, original error %w", err)
	}
	return nil, nil
}

func checkPackInner(ctx context.Context, r *Repository, id restic.ID, blobs pack.Blobs, size int64, bufRd *bufio.Reader, dec *zstd.Decoder) (goodBlobs []restic.BlobHandle, err error) {

	type partialReadError struct {
		error
	}

	debug.Log("checking pack %v", id.String())

	if len(blobs) == 0 {
		return nil, &ErrPackData{PackID: id, errs: []error{errors.New("pack is empty or not indexed")}}
	}

	// sanity check blobs in index
	blobs.Sort()
	idxHdrSize := pack.CalculateHeaderSize(blobs)
	lastBlobEnd := 0
	nonContinuousPack := false
	for _, blob := range blobs {
		if lastBlobEnd != int(blob.Offset) {
			nonContinuousPack = true
		}
		lastBlobEnd = int(blob.Offset + blob.Length)
	}
	// size was calculated by masterindex.PackSize, thus there's no need to recalculate it here

	var errs []error
	if nonContinuousPack {
		debug.Log("Index for pack contains gaps / overlaps, blobs: %v", blobs)
		errs = append(errs, errors.New("index for pack contains gaps / overlapping blobs"))
	}

	// per-blob verification results of this read attempt; salvageableBlobs
	// reports the blobs that were actually reached and verified successfully
	var checkedBlobs restic.BlobSet
	var badBlobs restic.BlobSet
	salvageableBlobs := func() []restic.BlobHandle {
		if checkedBlobs == nil {
			return nil
		}
		return checkedBlobs.Sub(badBlobs).List()
	}

	// calculate hash on-the-fly while reading the pack and capture pack header
	var hash restic.ID
	var hdrBuf []byte
	// must use a separate slice from `errs` here as we're only interested in the last retry
	var blobErrors []error
	h := backend.Handle{Type: backend.PackFile, Name: id.String()}
	err = r.be.Load(ctx, h, int(size), 0, func(rd io.Reader) error {
		hrd := hashing.NewReader(rd, sha256.New())
		bufRd.Reset(hrd)
		// reset blob errors for each retry
		blobErrors = nil
		checkedBlobs = nil
		badBlobs = nil

		it := newPackBlobIterator(id, newBufReader(bufRd), 0, blobs, r.Key(), dec)
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			val, err := it.Next()
			if err == errPackEOF {
				break
			} else if err != nil {
				return &partialReadError{err}
			}
			debug.Log("  check blob %v: %v", val.Handle.ID, val.Handle)
			if checkedBlobs == nil {
				checkedBlobs = restic.NewBlobSet()
			}
			checkedBlobs.Insert(val.Handle)
			if val.Err != nil {
				debug.Log("  error verifying blob %v: %v", val.Handle.ID, val.Err)
				if badBlobs == nil {
					badBlobs = restic.NewBlobSet()
				}
				badBlobs.Insert(val.Handle)
				blobErrors = append(blobErrors, errors.Errorf("blob %v: %v", val.Handle.ID, val.Err))
			}
		}

		// skip enough bytes until we reach the possible header start
		curPos := lastBlobEnd
		minHdrStart := int(size) - pack.MaxHeaderSize
		if minHdrStart > curPos {
			_, err := bufRd.Discard(minHdrStart - curPos)
			if err != nil {
				return &partialReadError{err}
			}
			curPos += minHdrStart - curPos
		}

		// read remainder, which should be the pack header
		var err error
		hdrBuf = make([]byte, int(size-int64(curPos)))
		_, err = io.ReadFull(bufRd, hdrBuf)
		if err != nil {
			return &partialReadError{err}
		}

		hash = restic.IDFromHash(hrd.Sum(nil))
		return nil
	})
	errs = append(errs, blobErrors...)
	if err != nil {
		var e *partialReadError
		isPartialReadError := errors.As(err, &e)
		// failed to load the pack file, return as further checks cannot succeed anyways
		debug.Log("  error streaming pack (partial %v): %v", isPartialReadError, err)
		if isPartialReadError {
			return salvageableBlobs(), &ErrPackData{PackID: id, errs: append(errs, fmt.Errorf("partial download error: %w", err))}
		}

		// The check command suggests to repair files for which a `ErrPackData` is returned. However, this file
		// completely failed to download such that there's no point in repairing anything.
		return nil, fmt.Errorf("download error: %w", err)
	}
	if !hash.Equal(id) {
		debug.Log("pack ID does not match, want %v, got %v", id, hash)
		return salvageableBlobs(), &ErrPackData{PackID: id, errs: append(errs, errors.Errorf("unexpected pack id %v", hash))}
	}

	blobs, hdrSize, err := pack.List(r.Key(), bytes.NewReader(hdrBuf), int64(len(hdrBuf)))
	if err != nil {
		return salvageableBlobs(), &ErrPackData{PackID: id, errs: append(errs, err)}
	}

	if uint32(idxHdrSize) != hdrSize {
		debug.Log("Pack header size does not match, want %v, got %v", idxHdrSize, hdrSize)
		errs = append(errs, errors.Errorf("pack header size does not match, want %v, got %v", idxHdrSize, hdrSize))
	}

	for _, blob := range blobs {
		// Check if blob is contained in index and position is correct
		idxHas := false
		for _, pb := range r.idx.Lookup(blob.BlobHandle) {
			if pb.PackID().Equal(id) && pb.Blob == blob {
				idxHas = true
				break
			}
		}
		if !idxHas {
			errs = append(errs, errors.Errorf("blob %v is not contained in index or position is incorrect", blob.ID))
			continue
		}
	}

	if len(errs) > 0 {
		return salvageableBlobs(), &ErrPackData{PackID: id, errs: errs}
	}

	return nil, nil
}

type bufReader struct {
	rd  *bufio.Reader
	buf []byte
}

func newBufReader(rd *bufio.Reader) *bufReader {
	return &bufReader{
		rd: rd,
	}
}

func (b *bufReader) Discard(n int) (discarded int, err error) {
	return b.rd.Discard(n)
}

func (b *bufReader) ReadFull(n int) (buf []byte, err error) {
	if cap(b.buf) < n {
		b.buf = make([]byte, n)
	}
	b.buf = b.buf[:n]

	_, err = io.ReadFull(b.rd, b.buf)
	if err != nil {
		return nil, err
	}
	return b.buf, nil
}
