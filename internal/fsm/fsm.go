// Package fsm implements the replicated key-value state machine that raft
// drives. Every node applies the same committed log entries in the same order,
// so the only requirement on this code is determinism: no clocks, no
// randomness, no I/O inside Apply.
package fsm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

// Result is what Apply returns to the proposer through the raft future.
type Result struct {
	// KV is the key state after a successful Put or CompareAndSwap, or the
	// current state on a failed CompareAndSwap (nil when the key is absent).
	KV *kvv1.KeyValue
	// Succeeded reports whether a CompareAndSwap matched.
	Succeeded bool
	// Deleted reports whether a Delete removed an existing key.
	Deleted bool
	// Err is set for a malformed command. It is deterministic across nodes.
	Err error
}

// Store is the in-memory key-value state machine.
type Store struct {
	mu      sync.RWMutex
	kvs     map[string]*kvv1.KeyValue
	applied uint64        // index of the last log entry applied (commands and snapshots)
	notify  chan struct{} // closed and replaced on every applied change
}

var _ raft.FSM = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{kvs: make(map[string]*kvv1.KeyValue), notify: make(chan struct{})}
}

// Apply is called by raft for every committed LogCommand entry, in log order,
// from a single goroutine.
func (s *Store) Apply(l *raft.Log) any {
	var cmd kvv1.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		s.advance(l.Index)
		return &Result{Err: fmt.Errorf("decode command at index %d: %w", l.Index, err)}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var res *Result
	switch op := cmd.GetOp().(type) {
	case *kvv1.Command_Put:
		res = &Result{KV: s.put(op.Put.GetKey(), op.Put.GetValue(), l.Index)}
	case *kvv1.Command_Delete:
		key := string(op.Delete.GetKey())
		_, ok := s.kvs[key]
		delete(s.kvs, key)
		res = &Result{Deleted: ok}
	case *kvv1.Command_Cas:
		res = s.cas(op.Cas, l.Index)
	default:
		res = &Result{Err: fmt.Errorf("unknown command at index %d", l.Index)}
	}
	s.advanceLocked(l.Index)
	return res
}

func (s *Store) put(key, value []byte, index uint64) *kvv1.KeyValue {
	k := string(key)
	cur, ok := s.kvs[k]
	next := &kvv1.KeyValue{Key: key, Value: value, Version: 1, CreateIndex: index, ModIndex: index}
	if ok {
		next.Version = cur.GetVersion() + 1
		next.CreateIndex = cur.GetCreateIndex()
	}
	s.kvs[k] = next
	return clone(next)
}

func (s *Store) cas(op *kvv1.CompareAndSwapOp, index uint64) *Result {
	cur, ok := s.kvs[string(op.GetKey())]
	switch {
	case op.GetExpectedVersion() == 0 && ok:
		return &Result{Succeeded: false, KV: clone(cur)}
	case op.GetExpectedVersion() != 0 && (!ok || cur.GetVersion() != op.GetExpectedVersion()):
		return &Result{Succeeded: false, KV: clone(cur)}
	}
	return &Result{Succeeded: true, KV: s.put(op.GetKey(), op.GetValue(), index)}
}

// Get returns a copy of the key, or nil when absent.
func (s *Store) Get(key []byte) *kvv1.KeyValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return clone(s.kvs[string(key)])
}

// Range returns up to limit keys with the given prefix, sorted, strictly after
// startAfter. more reports whether further keys exist.
func (s *Store) Range(prefix, startAfter []byte, limit int) (kvs []*kvv1.KeyValue, more bool) {
	s.mu.RLock()
	matches := make([]*kvv1.KeyValue, 0, 64)
	for k, kv := range s.kvs {
		if bytes.HasPrefix([]byte(k), prefix) && (len(startAfter) == 0 || k > string(startAfter)) {
			matches = append(matches, kv)
		}
	}
	s.mu.RUnlock()

	sort.Slice(matches, func(i, j int) bool { return bytes.Compare(matches[i].Key, matches[j].Key) < 0 })
	if limit > 0 && len(matches) > limit {
		matches, more = matches[:limit], true
	}
	kvs = make([]*kvv1.KeyValue, len(matches))
	for i, kv := range matches {
		kvs[i] = clone(kv)
	}
	return kvs, more
}

// Len returns the number of keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.kvs)
}

// Applied returns the index of the last log entry this store has applied.
// Unlike raft.AppliedIndex, this only moves once the state is visible to
// readers, which is what a linearizable read must wait for.
func (s *Store) Applied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// WaitApplied blocks until the store has applied at least index or ctx ends.
func (s *Store) WaitApplied(ctx context.Context, index uint64) error {
	for {
		s.mu.RLock()
		applied, ch := s.applied, s.notify
		s.mu.RUnlock()
		if applied >= index {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Store) advance(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(index)
}

func (s *Store) advanceLocked(index uint64) {
	if index > s.applied {
		s.applied = index
		close(s.notify)
		s.notify = make(chan struct{})
	}
}

// Snapshot captures the current state. It must be cheap because raft pauses
// applies while it runs; the copy is O(keys) and the encoding happens later in
// Persist, off the raft loop.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	kvs := make([]*kvv1.KeyValue, 0, len(s.kvs))
	for _, kv := range s.kvs {
		kvs = append(kvs, kv) // entries are never mutated in place, so sharing is safe
	}
	return &snapshot{kvs: kvs, applied: s.applied}, nil
}

// Restore replaces the state with a snapshot stream produced by Persist.
func (s *Store) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	header, kvs, err := decodeSnapshot(rc)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvs = kvs
	s.applied = header.GetLastAppliedIndex()
	close(s.notify)
	s.notify = make(chan struct{})
	return nil
}

func clone(kv *kvv1.KeyValue) *kvv1.KeyValue {
	if kv == nil {
		return nil
	}
	return proto.Clone(kv).(*kvv1.KeyValue)
}
