package api

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
)

// ReasonNotApplied is attached to every Unavailable error that this service
// itself produced before a proposal reached the log. A client that sees it
// knows the request had no effect. An Unavailable without it came from the
// transport (connection reset, server gone) and the outcome is unknown.
const ReasonNotApplied = "NOT_APPLIED"

// Error contract, also documented in the design doc:
//
//	Unavailable + NOT_APPLIED  the request was NOT applied; retry on any node.
//	Unavailable (transport)    unknown; the node died mid-request.
//	DeadlineExceeded           the outcome is UNKNOWN; the write may or may not commit.
//	FailedPrecondition         Partition/Join named an unknown peer, or similar.
//	InvalidArgument            bad key or value.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raftnode.ErrNotLeader), errors.Is(err, raftnode.ErrNoLeader):
		return notApplied(err.Error())
	case errors.Is(err, raftnode.ErrUnknownOutcome):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, raftnode.ErrUnknownPeer):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, err.Error())
	}
}

// notApplied builds the marked Unavailable error.
func notApplied(msg string) error {
	st, err := status.New(codes.Unavailable, msg).WithDetails(&errdetails.ErrorInfo{Reason: ReasonNotApplied, Domain: "kv.v1"})
	if err != nil {
		return status.Error(codes.Unavailable, msg)
	}
	return st.Err()
}

// IsNotApplied reports whether err guarantees the request had no effect.
func IsNotApplied(err error) bool {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == ReasonNotApplied {
			return true
		}
	}
	return false
}
