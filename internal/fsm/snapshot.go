package fsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

// snapshotFormatVersion is bumped when the on-disk layout changes.
const snapshotFormatVersion = 1

// maxRecordSize bounds a single length-prefixed record so a corrupt length
// cannot make Restore allocate unbounded memory.
const maxRecordSize = 64 << 20

type snapshot struct {
	kvs     []*kvv1.KeyValue
	applied uint64
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

// Persist streams the snapshot as a SnapshotHeader followed by one KeyValue
// per key, each as a uvarint length prefix plus protobuf bytes. Streaming keeps
// memory flat regardless of key count.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	w := bufio.NewWriter(sink)
	header := &kvv1.SnapshotHeader{
		FormatVersion:    snapshotFormatVersion,
		LastAppliedIndex: s.applied,
		NumKeys:          uint64(len(s.kvs)),
	}
	if err := writeRecord(w, header); err != nil {
		return cancel(sink, err)
	}
	for _, kv := range s.kvs {
		if err := writeRecord(w, kv); err != nil {
			return cancel(sink, err)
		}
	}
	if err := w.Flush(); err != nil {
		return cancel(sink, err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

func cancel(sink raft.SnapshotSink, err error) error {
	_ = sink.Cancel()
	return err
}

func writeRecord(w *bufio.Writer, m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(b)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readRecord(r *bufio.Reader, m proto.Message) error {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if size > maxRecordSize {
		return fmt.Errorf("snapshot record of %d bytes exceeds limit", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return proto.Unmarshal(buf, m)
}

func decodeSnapshot(rc io.Reader) (*kvv1.SnapshotHeader, map[string]*kvv1.KeyValue, error) {
	r := bufio.NewReader(rc)
	var header kvv1.SnapshotHeader
	if err := readRecord(r, &header); err != nil {
		return nil, nil, fmt.Errorf("read snapshot header: %w", err)
	}
	if header.GetFormatVersion() != snapshotFormatVersion {
		return nil, nil, fmt.Errorf("unsupported snapshot format %d", header.GetFormatVersion())
	}
	kvs := make(map[string]*kvv1.KeyValue, header.GetNumKeys())
	for i := uint64(0); i < header.GetNumKeys(); i++ {
		var kv kvv1.KeyValue
		if err := readRecord(r, &kv); err != nil {
			return nil, nil, fmt.Errorf("read snapshot record %d: %w", i, err)
		}
		kvs[string(kv.GetKey())] = &kv
	}
	return &header, kvs, nil
}
