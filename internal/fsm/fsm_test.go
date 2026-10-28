package fsm

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

func apply(t *testing.T, s *Store, index uint64, cmd *kvv1.Command) *Result {
	t.Helper()
	data, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := s.Apply(&raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: data}).(*Result)
	if !ok {
		t.Fatalf("Apply returned %T", res)
	}
	return res
}

func put(key, value string) *kvv1.Command {
	return &kvv1.Command{Op: &kvv1.Command_Put{Put: &kvv1.PutOp{Key: []byte(key), Value: []byte(value)}}}
}

func del(key string) *kvv1.Command {
	return &kvv1.Command{Op: &kvv1.Command_Delete{Delete: &kvv1.DeleteOp{Key: []byte(key)}}}
}

func cas(key string, expected uint64, value string) *kvv1.Command {
	return &kvv1.Command{Op: &kvv1.Command_Cas{Cas: &kvv1.CompareAndSwapOp{
		Key: []byte(key), ExpectedVersion: expected, Value: []byte(value),
	}}}
}

func TestPutTracksVersionAndIndexes(t *testing.T) {
	s := New()
	first := apply(t, s, 10, put("a", "1"))
	if first.KV.GetVersion() != 1 || first.KV.GetCreateIndex() != 10 || first.KV.GetModIndex() != 10 {
		t.Fatalf("unexpected first put: %v", first.KV)
	}
	second := apply(t, s, 11, put("a", "2"))
	if second.KV.GetVersion() != 2 || second.KV.GetCreateIndex() != 10 || second.KV.GetModIndex() != 11 {
		t.Fatalf("unexpected second put: %v", second.KV)
	}
	if got := s.Get([]byte("a")); string(got.GetValue()) != "2" {
		t.Fatalf("get after put = %q", got.GetValue())
	}
	if s.Applied() != 11 {
		t.Fatalf("applied = %d, want 11", s.Applied())
	}
}

func TestDelete(t *testing.T) {
	s := New()
	apply(t, s, 1, put("a", "1"))
	if res := apply(t, s, 2, del("a")); !res.Deleted {
		t.Fatal("expected deleted=true for existing key")
	}
	if res := apply(t, s, 3, del("a")); res.Deleted {
		t.Fatal("expected deleted=false for missing key")
	}
	if s.Get([]byte("a")) != nil {
		t.Fatal("key still present after delete")
	}
	// Re-creating a deleted key starts over at version 1.
	if res := apply(t, s, 4, put("a", "x")); res.KV.GetVersion() != 1 || res.KV.GetCreateIndex() != 4 {
		t.Fatalf("recreated key = %v", res.KV)
	}
}

func TestCompareAndSwap(t *testing.T) {
	s := New()
	tests := []struct {
		name     string
		cmd      *kvv1.Command
		wantOK   bool
		wantVal  string
		wantVers uint64
	}{
		{"create when absent", cas("k", 0, "v1"), true, "v1", 1},
		{"create when present fails", cas("k", 0, "v2"), false, "v1", 1},
		{"wrong version fails", cas("k", 5, "v2"), false, "v1", 1},
		{"right version succeeds", cas("k", 1, "v2"), true, "v2", 2},
		{"stale version fails", cas("k", 1, "v3"), false, "v2", 2},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := apply(t, s, uint64(i+1), tc.cmd)
			if res.Succeeded != tc.wantOK {
				t.Fatalf("succeeded = %v, want %v", res.Succeeded, tc.wantOK)
			}
			if string(res.KV.GetValue()) != tc.wantVal || res.KV.GetVersion() != tc.wantVers {
				t.Fatalf("kv = %v, want %s@%d", res.KV, tc.wantVal, tc.wantVers)
			}
		})
	}
	if res := apply(t, s, 100, cas("missing", 3, "x")); res.Succeeded || res.KV != nil {
		t.Fatalf("cas on missing key with version: %+v", res)
	}
}

func TestRangePrefixCursorLimit(t *testing.T) {
	s := New()
	for i, k := range []string{"b", "a/3", "a/1", "a/2", "c"} {
		apply(t, s, uint64(i+1), put(k, k))
	}
	kvs, more := s.Range([]byte("a/"), nil, 2)
	if !more || len(kvs) != 2 || string(kvs[0].Key) != "a/1" || string(kvs[1].Key) != "a/2" {
		t.Fatalf("page 1 = %v more=%v", kvs, more)
	}
	kvs, more = s.Range([]byte("a/"), kvs[1].Key, 2)
	if more || len(kvs) != 1 || string(kvs[0].Key) != "a/3" {
		t.Fatalf("page 2 = %v more=%v", kvs, more)
	}
	if kvs, _ := s.Range(nil, nil, 0); len(kvs) != 5 {
		t.Fatalf("unbounded range = %d keys", len(kvs))
	}
}

func TestMalformedCommandIsRejectedNotFatal(t *testing.T) {
	s := New()
	res := s.Apply(&raft.Log{Index: 7, Type: raft.LogCommand, Data: []byte{0xff, 0xff}}).(*Result)
	if res.Err == nil {
		t.Fatal("expected decode error")
	}
	if s.Applied() != 7 {
		t.Fatalf("applied = %d, want 7 even for rejected entries", s.Applied())
	}
}

func TestResultsAreCopies(t *testing.T) {
	s := New()
	res := apply(t, s, 1, put("a", "1"))
	res.KV.Value = []byte("mutated")
	if got := s.Get([]byte("a")); string(got.GetValue()) != "1" {
		t.Fatal("caller mutation leaked into the store")
	}
}

type memSink struct {
	bytes.Buffer
	closed, cancelled bool
}

func (m *memSink) ID() string    { return "mem" }
func (m *memSink) Close() error  { m.closed = true; return nil }
func (m *memSink) Cancel() error { m.cancelled = true; return nil }

func TestSnapshotRoundTrip(t *testing.T) {
	src := New()
	for i := range 1000 {
		apply(t, src, uint64(i+1), put(string(rune('a'+i%26))+"/"+string(rune('0'+i%10)), "v"))
	}
	apply(t, src, 1001, del("a/0"))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	if !sink.closed || sink.cancelled {
		t.Fatalf("sink closed=%v cancelled=%v", sink.closed, sink.cancelled)
	}

	dst := New()
	apply(t, dst, 1, put("stale", "should be dropped"))
	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if dst.Applied() != 1001 {
		t.Fatalf("restored applied = %d, want 1001", dst.Applied())
	}
	if dst.Get([]byte("stale")) != nil {
		t.Fatal("restore did not replace prior state")
	}
	want, _ := src.Range(nil, nil, 0)
	got, _ := dst.Range(nil, nil, 0)
	if len(want) != len(got) {
		t.Fatalf("restored %d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Fatalf("key %d differs: %v vs %v", i, want[i], got[i])
		}
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	if err := New().Restore(io.NopCloser(bytes.NewReader([]byte{0x05, 1, 2, 3, 4, 5}))); err == nil {
		t.Fatal("expected error")
	}
}

func TestWaitApplied(t *testing.T) {
	s := New()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.WaitApplied(ctx, 3) }()
	apply(t, s, 1, put("a", "1"))
	apply(t, s, 2, put("a", "2"))
	select {
	case err := <-done:
		t.Fatalf("WaitApplied returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	apply(t, s, 3, put("a", "3"))
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	short, cancelShort := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelShort()
	if err := s.WaitApplied(short, 99); err == nil {
		t.Fatal("expected context error")
	}
}
