// Package linearizability checks recorded client histories against a
// sequential key-value model with Porcupine. It is the stretch goal of the
// exercise: randomized faults (kill, partition, freeze) while clients run, then
// a proof that every acknowledged operation fits one legal sequential order.
package linearizability

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// Kind is the operation type.
type Kind int

// Operation kinds.
const (
	KindGet Kind = iota
	KindPut
	KindCas
)

func (k Kind) String() string {
	switch k {
	case KindGet:
		return "get"
	case KindPut:
		return "put"
	default:
		return "cas"
	}
}

// Input is the call side of an operation.
type Input struct {
	Kind     Kind
	Key      string
	Value    string // put / cas
	Expected uint64 // cas: expected version, 0 = must not exist
}

// Output is the return side. Unknown means the client got no definitive
// answer (timeout, connection lost): the operation may have taken effect at
// any later time, so its Return timestamp is set to infinity.
type Output struct {
	Unknown   bool
	Found     bool   // get
	Value     string // get
	Version   uint64 // get, put, successful cas
	Succeeded bool   // cas

	err string // error class, statistics only; not part of the model
}

// register is the sequential state of one key.
type register struct {
	Exists  bool
	Value   string
	Version uint64
}

func (r register) next(value string) register {
	if !r.Exists {
		return register{Exists: true, Value: value, Version: 1}
	}
	return register{Exists: true, Value: value, Version: r.Version + 1}
}

// Model is the Porcupine model: a map of independent registers, checked per
// key (Partition) so the search stays small.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var order []string
		for _, op := range history {
			k := op.Input.(Input).Key
			if _, ok := byKey[k]; !ok {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return register{} },
	Step: func(state, input, output any) (bool, any) {
		st, in, out := state.(register), input.(Input), output.(Output)
		switch in.Kind {
		case KindGet:
			if out.Unknown {
				return true, st
			}
			if out.Found != st.Exists {
				return false, st
			}
			return !st.Exists || (out.Value == st.Value && out.Version == st.Version), st
		case KindPut:
			next := st.next(in.Value)
			if out.Unknown {
				return true, next
			}
			return out.Version == next.Version, next
		default: // KindCas
			matches := (in.Expected == 0 && !st.Exists) || (st.Exists && st.Version == in.Expected)
			if out.Unknown {
				if matches {
					return true, st.next(in.Value)
				}
				return true, st
			}
			if out.Succeeded != matches {
				return false, st
			}
			if matches {
				next := st.next(in.Value)
				return out.Version == next.Version, next
			}
			return true, st
		}
	},
	Equal: func(a, b any) bool { return a.(register) == b.(register) },
	DescribeOperation: func(input, output any) string {
		in, out := input.(Input), output.(Output)
		switch {
		case out.Unknown:
			return fmt.Sprintf("%s(%s, %q, exp=%d) -> ?", in.Kind, in.Key, in.Value, in.Expected)
		case in.Kind == KindGet:
			if !out.Found {
				return fmt.Sprintf("get(%s) -> nil", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %q@%d", in.Key, out.Value, out.Version)
		case in.Kind == KindPut:
			return fmt.Sprintf("put(%s, %q) -> @%d", in.Key, in.Value, out.Version)
		default:
			return fmt.Sprintf("cas(%s, exp=%d, %q) -> %v@%d", in.Key, in.Expected, in.Value, out.Succeeded, out.Version)
		}
	},
	DescribeState: func(state any) string {
		st := state.(register)
		if !st.Exists {
			return "nil"
		}
		return fmt.Sprintf("%q@%d", st.Value, st.Version)
	},
}
