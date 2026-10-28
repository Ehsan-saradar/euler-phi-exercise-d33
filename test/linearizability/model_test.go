package linearizability

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func op(client int, in Input, out Output, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

func TestModelAcceptsSequentialHistory(t *testing.T) {
	h := []porcupine.Operation{
		op(0, Input{Kind: KindPut, Key: "a", Value: "1"}, Output{Version: 1}, 0, 10),
		op(1, Input{Kind: KindGet, Key: "a"}, Output{Found: true, Value: "1", Version: 1}, 20, 30),
		op(0, Input{Kind: KindCas, Key: "a", Expected: 1, Value: "2"}, Output{Succeeded: true, Version: 2}, 40, 50),
		op(1, Input{Kind: KindCas, Key: "a", Expected: 1, Value: "3"}, Output{Succeeded: false}, 60, 70),
		op(1, Input{Kind: KindGet, Key: "a"}, Output{Found: true, Value: "2", Version: 2}, 80, 90),
	}
	if !porcupine.CheckOperations(Model, h) {
		t.Fatal("legal history rejected")
	}
}

func TestModelRejectsStaleRead(t *testing.T) {
	h := []porcupine.Operation{
		op(0, Input{Kind: KindPut, Key: "a", Value: "1"}, Output{Version: 1}, 0, 10),
		op(1, Input{Kind: KindGet, Key: "a"}, Output{Found: false}, 20, 30), // read after an acked write misses it
	}
	if porcupine.CheckOperations(Model, h) {
		t.Fatal("stale read accepted")
	}
}

func TestModelRejectsLostUpdateOnVersions(t *testing.T) {
	h := []porcupine.Operation{
		op(0, Input{Kind: KindPut, Key: "a", Value: "1"}, Output{Version: 1}, 0, 10),
		op(1, Input{Kind: KindPut, Key: "a", Value: "2"}, Output{Version: 1}, 20, 30), // version did not advance
	}
	if porcupine.CheckOperations(Model, h) {
		t.Fatal("duplicate version accepted")
	}
}

func TestModelUnknownWriteMayOrMayNotHappen(t *testing.T) {
	const inf = int64(1) << 62
	// Unknown put observed by a later read: must be accepted (it happened).
	seen := []porcupine.Operation{
		op(0, Input{Kind: KindPut, Key: "a", Value: "1"}, Output{Unknown: true}, 0, inf),
		op(1, Input{Kind: KindGet, Key: "a"}, Output{Found: true, Value: "1", Version: 1}, 20, 30),
	}
	if !porcupine.CheckOperations(Model, seen) {
		t.Fatal("unknown write that took effect rejected")
	}
	// Unknown put never observed: also accepted (it may have been lost).
	lost := []porcupine.Operation{
		op(0, Input{Kind: KindPut, Key: "a", Value: "1"}, Output{Unknown: true}, 0, inf),
		op(1, Input{Kind: KindGet, Key: "a"}, Output{Found: false}, 20, 30),
	}
	if !porcupine.CheckOperations(Model, lost) {
		t.Fatal("unknown write that was lost rejected")
	}
	// A definite ack followed by a miss is still a violation.
	bad := []porcupine.Operation{
		op(0, Input{Kind: KindPut, Key: "a", Value: "1"}, Output{Version: 1}, 0, 10),
		op(1, Input{Kind: KindGet, Key: "a"}, Output{Found: false}, 20, 30),
	}
	if porcupine.CheckOperations(Model, bad) {
		t.Fatal("lost acknowledged write accepted")
	}
}
