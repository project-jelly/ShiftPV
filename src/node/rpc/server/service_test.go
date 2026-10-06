package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type executor func(context.Context, *protocol.EffectRequest, protocol.Operation) error

func (f executor) Execute(ctx context.Context, r *protocol.EffectRequest, o protocol.Operation) error {
	return f(ctx, r, o)
}

type pools struct{ pool volumeapi.Pool }

func (p pools) ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return p.pool, nil
}

type probe func(context.Context, volumeapi.Pool) (capacity.Filesystem, error)

func (p probe) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (capacity.Filesystem, error) {
	return p(ctx, pool)
}
func TestServiceChecksIdentityAuthorityAndBoundedDeadline(t *testing.T) {
	identity := volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "uid", NodeName: "worker-a"}
	req := &protocol.EffectRequest{ExecutorUid: identity.PodUID, NodeName: identity.NodeName, VolumeName: "volume", VolumeUid: "volume-uid", OperationId: "operation"}
	calls := 0
	s := &Service{Identity: identity, Executor: executor(func(_ context.Context, _ *protocol.EffectRequest, o protocol.Operation) error {
		calls++
		if o != protocol.Operation_CREATE && o != protocol.Operation_RECLAIM {
			t.Fatal("unexpected operation")
		}
		return nil
	})}
	if _, err := s.CreateCopy(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReclaimCopy(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*protocol.EffectRequest){func(r *protocol.EffectRequest) { r.ExecutorUid = "old" }, func(r *protocol.EffectRequest) { r.NodeName = "other" }, func(r *protocol.EffectRequest) { r.OperationId = "" }} {
		altered := &protocol.EffectRequest{ExecutorUid: req.ExecutorUid, NodeName: req.NodeName, VolumeName: req.VolumeName, VolumeUid: req.VolumeUid, OperationId: req.OperationId}
		change(altered)
		if _, err := s.CreateCopy(context.Background(), altered); err == nil {
			t.Fatal("invalid request executed")
		}
	}
	if _, err := s.CreateCopy(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("invalid authority reached executor")
	}
	for _, method := range []string{protocol.Node_GetCapacity_FullMethodName, protocol.Node_CreateCopy_FullMethodName} {
		_, err := boundDeadline(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, _ any) (any, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("unbounded RPC")
			}
			limit := 2 * time.Minute
			if method == protocol.Node_GetCapacity_FullMethodName {
				limit = 2 * time.Second
			}
			if time.Until(deadline) > limit {
				t.Fatal("wrong deadline")
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestCapacityVerifiesBeforeAndAfterFilesystemRead(t *testing.T) {
	identity := volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "uid", NodeName: "worker-a"}
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: identity.NodeName, MountPath: "/mnt/a", Generation: 1}
	evidence, _ := capacity.MeasurementEvidence(pool)
	req := &protocol.CapacityRequest{ExecutorUid: identity.PodUID, NodeName: identity.NodeName, PoolName: pool.Name, PoolUid: pool.UID, Evidence: evidence, Nonce: "0123456789abcdef0123456789abcdef"}
	checks, reads := 0, 0
	s := &Service{Identity: identity, Pools: pools{pool}, Probe: probe(func(context.Context, volumeapi.Pool) (capacity.Filesystem, error) {
		reads++
		return capacity.Filesystem{TotalBytes: 10, AvailableBytes: 5}, nil
	}), VerifyIdentity: func(context.Context) error { checks++; return nil }}
	answer, err := s.GetCapacity(context.Background(), req)
	if err != nil || answer.AvailableBytes != 5 || checks != 2 || reads != 1 {
		t.Fatalf("answer=%v error=%v checks=%d reads=%d", answer, err, checks, reads)
	}
	s.VerifyIdentity = func(context.Context) error {
		checks++
		if checks%2 == 0 {
			return volumeapi.ErrStateConflict
		}
		return nil
	}
	if _, err := s.GetCapacity(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retired during read=%v", err)
	}
	req.Evidence = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	s.VerifyIdentity = nil
	if _, err := s.GetCapacity(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale Pool=%v", err)
	}
	if reads != 2 {
		t.Fatal("stale Pool reached probe")
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, volumeapi.ErrStateConflict, errors.New("offline")} {
		if rpcError(err) == nil {
			t.Fatal("failure became success")
		}
	}
}
