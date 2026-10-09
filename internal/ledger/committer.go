package ledger

import (
	"sync"
	"time"

	"github.com/restic/restic/internal/restic"
)

const (
	// DefaultCommitBatchSize is the number of records a Committer collects
	// before forcing an fsynced write.
	DefaultCommitBatchSize = 256
	// DefaultCommitInterval bounds how long a partially filled batch waits.
	DefaultCommitInterval = 250 * time.Millisecond
)

type opKind uint8

const (
	kindPut opKind = iota
	kindDelete
)

type queuedOp struct {
	kind opKind
	rec  Record
	id   restic.ID
	ack  chan error
}

type flushReq struct {
	ack chan error
}

// Committer serializes ledger mutations onto a single background goroutine and
// acknowledges each mutation only after its batch has been written and fsynced.
// Callers can therefore treat a nil error as durable progress and abort the
// surrounding operation on the first failure.
type Committer struct {
	store     *Store
	ch        chan queuedOp
	flushCh   chan flushReq
	stopCh    chan struct{}
	doneCh    chan struct{}
	batchSize int
	interval  time.Duration

	// inflight tracks senders that passed admission; it guarantees that
	// shutdown never closes the input channels while a send is in progress.
	inflight sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	fatalErr error
}

// NewCommitter starts a Committer with the default batching parameters.
func NewCommitter(store *Store) *Committer {
	return newCommitter(store, DefaultCommitBatchSize, DefaultCommitInterval)
}

func newCommitter(store *Store, batchSize int, interval time.Duration) *Committer {
	c := &Committer{
		store:     store,
		ch:        make(chan queuedOp, batchSize*2),
		flushCh:   make(chan flushReq, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		batchSize: batchSize,
		interval:  interval,
	}
	go c.run()
	return c
}

// Submit queues an upsert and blocks until it is durable.
func (c *Committer) Submit(rec Record) error {
	op := queuedOp{kind: kindPut, rec: rec, ack: make(chan error, 1)}
	if err := c.admit(); err != nil {
		return err
	}
	c.ch <- op
	// admission covers only the send; once it completed, Close may close ch
	c.inflight.Done()
	return <-op.ack
}

// SubmitDelete queues a deletion and blocks until it is durable.
func (c *Committer) SubmitDelete(id restic.ID) error {
	op := queuedOp{kind: kindDelete, id: id, ack: make(chan error, 1)}
	if err := c.admit(); err != nil {
		return err
	}
	c.ch <- op
	c.inflight.Done()
	return <-op.ack
}

// SubmitVerified queues a successful read through the batching pipeline while
// applying the same quarantine-to-verified rules as Store.MarkVerified. It is a
// no-op when the existing record must be kept untouched.
func (c *Committer) SubmitVerified(id restic.ID, fp Fingerprint) error {
	c.store.mu.Lock()
	existing, exists := c.store.records[id]
	c.store.mu.Unlock()

	rec, write := verifiedTransition(id, existing, exists, fp)
	if !write {
		return nil
	}
	return c.Submit(rec)
}

// Flush forces the current batch to be written and blocks until it is durable.
func (c *Committer) Flush() error {
	req := flushReq{ack: make(chan error, 1)}
	if err := c.admit(); err != nil {
		return err
	}
	c.flushCh <- req
	c.inflight.Done()
	return <-req.ack
}

func (c *Committer) admit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.fatalErr != nil {
		return c.fatalErr
	}
	c.inflight.Add(1)
	return nil
}

// Close flushes all queued mutations and stops the background goroutine. A
// persistence error is returned while all admitted operations are still
// acknowledged.
func (c *Committer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	close(c.stopCh)
	// wait until every admitted sender has finished its channel send ...
	c.inflight.Wait()
	// ... only then close the inputs, so no send can hit a closed channel
	close(c.ch)
	close(c.flushCh)
	<-c.doneCh

	c.mu.Lock()
	err := c.fatalErr
	c.mu.Unlock()
	return err
}

func (c *Committer) run() {
	defer close(c.doneCh)

	var batch []queuedOp
	for {
		select {
		case op := <-c.ch:
			batch = append(batch, op)
		case fr, ok := <-c.flushCh:
			if !ok {
				c.drain(&batch)
				return
			}
			batch = c.drainQueued(batch)
			fr.ack <- c.apply(batch)
			batch = nil
			continue
		case <-c.stopCh:
			c.drain(&batch)
			return
		}

		deadline := time.NewTimer(c.interval)
	collect:
		for len(batch) < c.batchSize {
			select {
			case op := <-c.ch:
				batch = append(batch, op)
			case fr, ok := <-c.flushCh:
				if !ok {
					deadline.Stop()
					c.drain(&batch)
					return
				}
				// a flush is a barrier: also persist everything queued so far
				batch = c.drainQueued(batch)
				fr.ack <- c.apply(batch)
				batch = nil
				deadline.Stop()
				break collect
			case <-deadline.C:
				break collect
			case <-c.stopCh:
				deadline.Stop()
				c.drain(&batch)
				return
			}
		}
		deadline.Stop()
		if len(batch) > 0 {
			c.apply(batch)
			batch = nil
		}
	}
}

// drainQueued appends operations that are already buffered in the input
// channel without blocking, so that a flush forms a persistence barrier.
func (c *Committer) drainQueued(batch []queuedOp) []queuedOp {
	for {
		select {
		case op := <-c.ch:
			batch = append(batch, op)
		default:
			return batch
		}
	}
}

// drain persists all operations that arrive until Close closes the inputs.
func (c *Committer) drain(batch *[]queuedOp) {
	// ch is closed only after every admitted sender has finished sending
	for {
		select {
		case op, ok := <-c.ch:
			if !ok {
				c.drainFlushes(batch)
				return
			}
			*batch = append(*batch, op)
			if len(*batch) >= c.batchSize {
				c.apply(*batch)
				*batch = nil
			}
		case fr, ok := <-c.flushCh:
			// Close closes ch before flushCh, so a closed flushCh means no
			// further ops can arrive
			if !ok {
				if len(*batch) > 0 {
					c.apply(*batch)
					*batch = nil
				}
				return
			}
			fr.ack <- c.apply(*batch)
			*batch = nil
		}
	}
}

// drainFlushes answers admitted flushes after the op channel has been closed,
// applying the trailing partial batch on each flush.
func (c *Committer) drainFlushes(batch *[]queuedOp) {
	for {
		select {
		case fr, ok := <-c.flushCh:
			if !ok {
				if len(*batch) > 0 {
					c.apply(*batch)
					*batch = nil
				}
				return
			}
			fr.ack <- c.apply(*batch)
			*batch = nil
		}
	}
}

func (c *Committer) apply(batch []queuedOp) error {
	upserts := make([]Record, 0, len(batch))
	var deletes []restic.ID
	for _, op := range batch {
		switch op.kind {
		case kindPut:
			upserts = append(upserts, op.rec)
		case kindDelete:
			deletes = append(deletes, op.id)
		}
	}

	err := c.store.Apply(upserts, deletes)
	for _, op := range batch {
		op.ack <- err
	}
	if err != nil {
		c.mu.Lock()
		if c.fatalErr == nil {
			c.fatalErr = err
		}
		c.mu.Unlock()
	}
	return err
}
