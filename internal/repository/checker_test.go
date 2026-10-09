package repository

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/repository/index"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

var checkerTestData = filepath.Join("..", "checker", "testdata", "checker-test-repo.tar.gz")

func testWrapCheckPack(ctx context.Context, t testing.TB, repo *Repository,
	packID restic.ID, blobs pack.Blobs, size int64,
) error {
	t.Helper()
	bufRd := bufio.NewReaderSize(nil, maxStreamBufferSize)
	dec, err := zstd.NewReader(nil)
	rtest.OK(t, err)

	_, err = checkPack(ctx, repo, packID, blobs, size, bufRd, dec)
	return err
}

// TestGapInBlobs creates a gap in the blob list by omitting the first entry before passing it to checkPack
func TestGapInBlobs(t *testing.T) {
	TestInjectKey(t, restic.TestParseID("7bb3065bfb17da7430dc4dde4741d6db3dd83fdb0829500cf105755e067f879a"), `{"mac":{"k":"W1Y8bmQNJg6TAmuDt7lbpQ==","r":"r43DBmAdmwtQneoBTGAABQ=="},"encrypt":"JuZGBs6joRiLzqkyMWhmbZMLHe8+5oH6MDE5I6M8R/I="}`)
	repo, _ := TestFromFixture(t, checkerTestData)

	err := repo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory)
	rtest.OK(t, err)

	repoPacks, err := pack.Size(context.TODO(), repo, false)
	rtest.OK(t, err)

	packID := restic.TestParseID("19a731a515618ec8b75fc0ff3b887d8feb83aef1001c9899f6702761142ed068")
	_, ok := repoPacks[packID]
	rtest.Assert(t, ok, "expected pack 19a731a515618ec8b75fc0ff3b887d8feb83aef1001c9899f6702761142ed068")

	blobs := pack.Blobs{}
	pb := <-repo.listPacksFromIndex(context.TODO(), restic.NewIDSet(packID))
	blobs = append(blobs, pb.Blobs...)

	// assertion for clarity, actually can't fail as the packfile content is fixed
	rtest.Assert(t, len(blobs) >= 2, "need at least 2 blobs in packfile 19a731a51")
	blobs = blobs[1:]
	err = testWrapCheckPack(context.TODO(), t, repo, packID, blobs, repoPacks[packID])

	var packErr *ErrPackData
	rtest.Assert(t, errors.As(err, &packErr), "expected ErrPackData, got: %T %v", err, err)
	rtest.Equals(t, packID, packErr.PackID)

	errText := err.Error()
	rtest.Assert(t, strings.Contains(errText, "gaps") || strings.Contains(errText, "overlapping"),
		"expected gap/overlap error in: %v", errText)
	rtest.Assert(t, strings.Contains(errText, "pack header size does not match"),
		"expected header size mismatch error in: %v", errText)
}

// helper functions for backend error fails

// collectErrors collects errors from checker methods
func collectErrors(ctx context.Context, f func(context.Context, chan<- error)) (errs []error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error)

	go f(ctx, errChan)

	for err := range errChan {
		errs = append(errs, err)
	}

	return errs
}

// runReadPacks calls ReadPacks which loads data from specified packs and checks the integrity
func runReadPacks(chkr *Checker) []error {
	return collectErrors(context.TODO(),
		func(ctx context.Context, errCh chan<- error) {
			chkr.ReadPacks(ctx, func(packs map[restic.ID]int64) map[restic.ID]int64 {
				return packs
			}, restic.NewNoopPrinter(), errCh, nil)
		})
}

// lastByteFlipBackend flips the last byte of every pack file on read,
// causing the SHA-256 hash computed by checkPackInner to differ from the
// content-addressed pack ID stored in the filename.
type lastByteFlipBackend struct {
	backend.Backend
}

func (b *lastByteFlipBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, consumer func(rd io.Reader) error) error {
	if h.Type != backend.PackFile {
		return b.Backend.Load(ctx, h, length, offset, consumer)
	}
	return b.Backend.Load(ctx, h, length, offset, func(rd io.Reader) error {
		buf, err := io.ReadAll(rd)
		if err != nil {
			return err
		}
		if len(buf) > 0 {
			buf[len(buf)-1] ^= 0xff
		}
		return consumer(bytes.NewReader(buf))
	})
}

// alwaysFailBackend returns a hard, non-partial error for every pack file
// load, simulating a complete download failure (e.g. network unreachable).

type alwaysFailBackend struct {
	backend.Backend
}

func (b *alwaysFailBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, consumer func(rd io.Reader) error) error {
	if h.Type == backend.PackFile {
		return errors.New("simulated total download failure")
	}
	return b.Backend.Load(ctx, h, length, offset, consumer)
}

// truncatingBackend returns only the first 8 bytes of every pack file,
// guaranteeing io.ErrUnexpectedEOF inside checkPackInner (a partial read).
type truncatingBackend struct {
	backend.Backend
}

func (b *truncatingBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, consumer func(rd io.Reader) error) error {
	if h.Type != backend.PackFile {
		return b.Backend.Load(ctx, h, length, offset, consumer)
	}
	return b.Backend.Load(ctx, h, length, offset, func(rd io.Reader) error {
		buf := make([]byte, 8)
		n, _ := io.ReadFull(rd, buf)
		return consumer(bytes.NewReader(buf[:n]))
	})
}

// blobCorruptBackend flips one byte inside the ciphertext of one specific blob
// in one specific pack, leaving the pack header (and all other blobs) intact.
type blobCorruptBackend struct {
	backend.Backend
	packID string
	offset uint
}

func (b *blobCorruptBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, consumer func(rd io.Reader) error) error {
	if h.Type != backend.PackFile || h.Name != b.packID {
		return b.Backend.Load(ctx, h, length, offset, consumer)
	}
	return b.Backend.Load(ctx, h, length, offset, func(rd io.Reader) error {
		buf, err := io.ReadAll(rd)
		if err != nil {
			return err
		}
		if int(b.offset) < len(buf) {
			buf[b.offset] ^= 0xff
		}
		return consumer(bytes.NewReader(buf))
	})
}

// runReadPacksWithResults calls ReadPacks and collects every PackCheckResult
// delivered to the hook.
func runReadPacksWithResults(t testing.TB, chkr *Checker) (results []PackCheckResult, errs []error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	errChan := make(chan error)
	resChan := make(chan PackCheckResult)
	var collectors sync.WaitGroup
	collectors.Add(2)
	go func() {
		defer collectors.Done()
		for err := range errChan {
			errs = append(errs, err)
		}
	}()
	go func() {
		defer collectors.Done()
		for res := range resChan {
			results = append(results, res)
		}
	}()

	var wg sync.WaitGroup
	wg.Go(func() {
		// ReadPacks closes errChan itself; close the result channel once it
		// has returned.
		defer close(resChan)
		chkr.ReadPacks(ctx, func(packs map[restic.ID]int64) map[restic.ID]int64 {
			return packs
		}, restic.NewNoopPrinter(), errChan, func(res PackCheckResult) error {
			resChan <- res
			return nil
		})
	})
	wg.Wait()
	collectors.Wait()
	return results, errs
}

// TestCheckPackGoodBlobsSingleCorruption verifies that a damaged pack reports
// exactly the cryptographically intact blobs in GoodBlobs (TR-3.1/3.2).
func TestCheckPackGoodBlobsSingleCorruption(t *testing.T) {
	repo, be := TestRepositoryWithBackend(t, nil, 0, Options{})
	data.TestCreateSnapshot(t, repo, time.Unix(1470492820, 207401672), 2)
	rtest.OK(t, repo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))

	allPacks, err := pack.Size(context.TODO(), repo, false)
	rtest.OK(t, err)
	packWhitelist := restic.NewIDSet()
	for id := range allPacks {
		packWhitelist.Insert(id)
	}

	var victimPack restic.ID
	var victim restic.BlobHandle
	var corruptAt uint
	allHandles := restic.NewBlobSet()
	for pbs := range repo.listPacksFromIndex(context.TODO(), packWhitelist) {
		b0 := pbs.Blobs[0]
		// the flipped byte must lie inside the blob ciphertext
		if int(b0.Offset+32) >= int(b0.Offset+b0.Length) {
			continue
		}
		victimPack = pbs.PackID
		victim = b0.BlobHandle
		corruptAt = b0.Offset + 32 // inside the ciphertext, past the nonce
		for _, b := range pbs.Blobs {
			allHandles.Insert(b.BlobHandle)
		}
		break
	}
	rtest.Assert(t, victimPack != restic.ID{}, "expected a pack with a readable blob")

	checkRepo := TestOpenBackend(t, &blobCorruptBackend{Backend: be, packID: victimPack.String(), offset: corruptAt})
	rtest.OK(t, checkRepo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	chkr := newChecker(checkRepo)

	results, errs := runReadPacksWithResults(t, chkr)
	rtest.Assert(t, len(errs) > 0, "expected the corrupted pack to be reported: %v", errs)

	var damaged *PackCheckResult
	for i := range results {
		res := &results[i]
		var packErr *ErrPackData
		if errors.As(res.Err, &packErr) && res.ID.Equal(victimPack) {
			damaged = res
			continue
		}
		if res.Err == nil {
			rtest.Assert(t, res.GoodBlobs == nil, "undamaged packs must report no GoodBlobs")
		}
	}
	rtest.Assert(t, damaged != nil, "expected an ErrPackData result for pack %v, got %+v", victimPack, results)

	gotGood := restic.NewBlobSet(damaged.GoodBlobs...)
	rtest.Equals(t, allHandles.Len()-1, gotGood.Len())
	rtest.Assert(t, !gotGood.Has(victim), "corrupted blob must not be salvageable: %v", victim)

	// hash-mismatch-only damage (last byte flip): all blobs verified, header
	// hash mismatches -> every blob must still be reported salvageable
	chkr2 := setupChecker(t, func(be backend.Backend) backend.Backend {
		return &lastByteFlipBackend{Backend: be}
	})
	results2, errs2 := runReadPacksWithResults(t, chkr2)
	rtest.Assert(t, len(errs2) > 0, "expected hash mismatch errors, got none")
	foundIntact := false
	for i := range results2 {
		if results2[i].Err != nil && results2[i].GoodBlobs != nil {
			foundIntact = true
		}
	}
	rtest.Assert(t, foundIntact, "hash mismatch pack should still list verified blobs")
}

// TestCheckPackGoodBlobsUnreadablePack verifies that packs that cannot be read
// far enough to verify any blob report no salvageable blobs (TR-3.3).
func TestCheckPackGoodBlobsUnreadablePack(t *testing.T) {
	for _, wrap := range []func(backend.Backend) backend.Backend{
		func(be backend.Backend) backend.Backend { return &truncatingBackend{Backend: be} },
		func(be backend.Backend) backend.Backend { return &alwaysFailBackend{Backend: be} },
	} {
		chkr := setupChecker(t, wrap)
		results, errs := runReadPacksWithResults(t, chkr)
		rtest.Assert(t, len(errs) > 0, "expected errors, got none")
		for i := range results {
			if results2 := results[i]; results2.Err != nil {
				rtest.Assert(t, results2.GoodBlobs == nil,
					"unreadable pack must not report salvageable blobs, got %d", len(results2.GoodBlobs))
			}
		}
	}
}

// TestReadPacksHookResults verifies the hook receives exactly one result per
// verified pack (TR-4.1) and that a hook error aborts the run (TR-4.2).
func TestReadPacksHookResults(t *testing.T) {
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend { return be })

	fps, err := chkr.PackFingerprints(context.TODO())
	rtest.OK(t, err)
	rtest.Assert(t, len(fps) > 0, "expected indexed packs")

	results, errs := runReadPacksWithResults(t, chkr)
	rtest.Equals(t, 0, len(errs))
	rtest.Equals(t, len(fps), len(results))
	seen := restic.NewIDSet()
	for _, res := range results {
		rtest.Assert(t, !seen.Has(res.ID), "duplicate hook result for %v", res.ID)
		seen.Insert(res.ID)
		rtest.Assert(t, res.Err == nil, "healthy pack must not report error")
		_, ok := fps[res.ID]
		rtest.Assert(t, ok, "hook result for unknown pack %v", res.ID)
	}
}

// TestReadPacksHookAbort verifies that a hook error stops the read.
func TestReadPacksHookAbort(t *testing.T) {
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend { return be })
	fps, err := chkr.PackFingerprints(context.TODO())
	rtest.OK(t, err)
	rtest.Assert(t, len(fps) > 1, "need several packs for the abort test")

	stopErr := errors.New("hook asked to stop")
	var called int
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	errChan := make(chan error)
	var wg sync.WaitGroup
	wg.Go(func() {
		// ReadPacks closes errChan
		chkr.ReadPacks(ctx, func(packs map[restic.ID]int64) map[restic.ID]int64 {
			return packs
		}, restic.NewNoopPrinter(), errChan, func(PackCheckResult) error {
			called++
			return stopErr
		})
	})
	var sawAbort bool
	var gotErrs []string
	for err := range errChan {
		gotErrs = append(gotErrs, err.Error())
		if strings.Contains(err.Error(), stopErr.Error()) {
			sawAbort = true
		}
	}
	wg.Wait()
	t.Logf("hook calls: %d, errors: %v", called, gotErrs)
	rtest.Assert(t, sawAbort, "expected the hook error to surface, results processed: %d", called)
	rtest.Assert(t, called <= len(fps), "hook must not be called for unknown packs: %d/%d", called, len(fps))
	// ReadPacks must have returned promptly instead of blocking; packs already
	// in flight can finish, but no result is delivered after the hook errors
	// (the errgroup cancels every remaining worker).
}

// TestPackFingerprints verifies determinism and index-change sensitivity
// (TR-4.3).
func TestPackFingerprints(t *testing.T) {
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend { return be })

	first, err := chkr.PackFingerprints(context.TODO())
	rtest.OK(t, err)
	second, err := chkr.PackFingerprints(context.TODO())
	rtest.OK(t, err)
	rtest.Equals(t, len(first), len(second))
	for id, fp := range first {
		rtest.Assert(t, fp == second[id], "fingerprint for %v changed between calls", id)
	}

	// PackBlobsHash must react to every component of an index entry
	id := restic.NewRandomID()
	base := index.PackBlobs{PackID: id, Blobs: pack.Blobs{
		{BlobHandle: restic.BlobHandle{ID: restic.NewRandomID(), Type: restic.DataBlob}, Length: 100, Offset: 0},
		{BlobHandle: restic.BlobHandle{ID: restic.NewRandomID(), Type: restic.DataBlob}, Length: 200, Offset: 100},
	}}
	h0 := index.PackBlobsHash(base)

	moved := base
	moved.Blobs = append(pack.Blobs(nil), base.Blobs...)
	moved.Blobs[1].Offset = 101
	rtest.Assert(t, index.PackBlobsHash(moved) != h0, "changed offset must change the fingerprint")

	resized := base
	resized.Blobs = append(pack.Blobs(nil), base.Blobs...)
	resized.Blobs[1].Length = 201
	rtest.Assert(t, index.PackBlobsHash(resized) != h0, "changed length must change the fingerprint")

	rtest.Equals(t, h0, index.PackBlobsHash(base), "unchanged input must hash identically")
}

// setupChecker creates a repository with one snapshot, then re-opens the
// same backend through the given wrapper for use by the checker.
func setupChecker(t *testing.T, wrap func(backend.Backend) backend.Backend) *Checker {
	t.Helper()
	// Write a snapshot into a fresh in-memory repository.
	repo, be := TestRepositoryWithBackend(t, nil, 0, Options{})
	data.TestCreateSnapshot(t, repo, time.Unix(1470492820, 207401672), 2)

	// Re-open the same backend (now containing real pack files) through
	// the corruption wrapper so the checker reads corrupted data.
	checkRepo := TestOpenBackend(t, wrap(be))
	chkr := newChecker(checkRepo)

	// make sure the index is loaded
	err := checkRepo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory)
	rtest.OK(t, err)

	return chkr
}

// TestCheckPackHashMismatch verifies that checkPackInner detects when the
// bytes stored in the backend don't hash to the pack's content-addressed ID.
// Covers the `!hash.Equal(id)` branch → ErrPackData "unexpected pack id".
func TestCheckPackHashMismatch(t *testing.T) {
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend {
		return &lastByteFlipBackend{Backend: be}
	})

	found := false
	dataErrs := runReadPacks(chkr)
	for _, err := range dataErrs {
		if strings.Contains(err.Error(), "unexpected pack id") {
			found = true
		}
	}
	rtest.Assert(t, found, "expected 'unexpected pack id' error, got: %v", dataErrs)
}

// TestCheckPackDownloadError verifies that a complete (non-partial) backend
// load failure is returned as a plain "download error" and NOT as ErrPackData.
func TestCheckPackDownloadError(t *testing.T) {
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend {
		return &alwaysFailBackend{Backend: be}
	})

	dataErrs := runReadPacks(chkr)
	rtest.Assert(t, len(dataErrs) > 0, "expected download errors, got none")

	for _, err := range dataErrs {
		var packErr *ErrPackData
		rtest.Assert(t, !errors.As(err, &packErr),
			"complete download failure must NOT produce ErrPackData, got: %v", err)
		rtest.Assert(t, strings.Contains(err.Error(), "download error"),
			"expected 'download error' in message, got: %v", err)
	}
}

// TestCheckPackPartialDownloadError verifies that a partial read (truncated
// response) is returned as ErrPackData, so the check command can suggest
// `restic repair packs` for the affected pack.
// Covers the partialReadError branch of the be.Load error path.
func TestCheckPackPartialDownloadError(t *testing.T) {
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend {
		return &truncatingBackend{Backend: be}
	})

	dataErrs := runReadPacks(chkr)
	rtest.Assert(t, len(dataErrs) > 0, "expected errors from truncated reads, got none")

	for _, err := range dataErrs {
		var packErr *ErrPackData
		rtest.Assert(t, errors.As(err, &packErr),
			"partial read must produce ErrPackData, got: %T %v", err, err)
	}
}

// warmupBackend simulates a backend where all handles needs to be warmed up.
type warmupBackend struct {
	backend.Backend
	handlesToWarmup []backend.Handle
	handlesAwaited  []backend.Handle
}

func (be *warmupBackend) Warmup(_ context.Context, h []backend.Handle) ([]backend.Handle, error) {
	be.handlesToWarmup = append(be.handlesToWarmup, h...)
	return h, nil
}

func (be *warmupBackend) WarmupWait(_ context.Context, h []backend.Handle) error {
	be.handlesAwaited = append(be.handlesAwaited, h...)
	return nil
}

func TestCheckerWarmup(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.S3Restore, true)()

	var wBackend *warmupBackend
	chkr := setupChecker(t, func(be backend.Backend) backend.Backend {
		wBackend = &warmupBackend{Backend: be}
		return wBackend
	})

	errs := runReadPacks(chkr)
	rtest.Assert(t, len(errs) == 0, "expected no data error, got %v: %v", len(errs), errs)

	rtest.Assert(t, len(wBackend.handlesToWarmup) > 0, "found no handles to warmup")
	rtest.Equals(t, wBackend.handlesToWarmup, wBackend.handlesAwaited, "expected to wait for all cold handles")
}
