package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/executor"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	rpcserver "github.com/project-jelly/ShiftPV/src/node/rpc/server"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/measurement"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// Permits make exhaustion and recovery deterministic; no wall-clock QPS or
// performance threshold decides whether this regression passes.
type creationBudget struct {
	flowcontrol.RateLimiter
	permits chan struct{}
	blocked chan struct{}
	once    sync.Once
	used    atomic.Int64
}

func (b *creationBudget) Wait(ctx context.Context) error {
	select {
	case <-b.permits:
		b.used.Add(1)
		return nil
	default:
		b.once.Do(func() { close(b.blocked) })
	}
	select {
	case <-b.permits:
		b.used.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *creationBudget) reset(permits int) {
	for len(b.permits) > 0 {
		<-b.permits
	}
	b.used.Store(0)
	b.release(permits)
}

func (b *creationBudget) release(permits int) {
	for range permits {
		b.permits <- struct{}{}
	}
}

// Both independent Pools have their own roots and approved block allocations.
// The real REST clients, RPC authorization, executor and filesystem receipts
// share the effects role budget just as they do in the Node process.
func TestNodeCreationSharesBoundedBudgetAcrossPools(t *testing.T) {
	for _, streams := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d-admission-streams", streams), func(t *testing.T) {
			fixture := newCreationFixture(t)
			defer fixture.server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			configs := newNodeAPIConfigs(&rest.Config{Host: fixture.server.URL, ContentConfig: rest.ContentConfig{ContentType: "application/json"}}, nil)
			budget := &creationBudget{RateLimiter: flowcontrol.NewFakeAlwaysRateLimiter(), permits: make(chan struct{}, 800), blocked: make(chan struct{})}
			budget.release(100)
			configs.effects.RateLimiter = budget
			clients := newNodeAPIClients(configs, func(step string, err error) { t.Fatalf("%s: %v", step, err) })
			if clients.effects.Typed.CoreV1().RESTClient().GetRateLimiter() != budget {
				t.Fatal("typed effects requests escaped the shared budget")
			}
			registry := &volumeapi.Registry{Client: clients.effects.Dynamic}
			pools := make([]volumeapi.Pool, 2)
			for i := range pools {
				var err error
				pools[i], err = registry.ReadyPoolForIdentity(ctx, fmt.Sprintf("pool-%d", i), fmt.Sprintf("pool-%d-uid", i), fixture.identity.NodeName)
				if err != nil {
					t.Fatalf("independent fixture Pool is not Ready: %v", err)
				}
			}
			node := &executor.Node{Identity: fixture.identity, Pods: clients.effects.Typed.CoreV1().Pods(fixture.identity.Namespace), Volumes: registry,
				Cleanups: &cleanupapi.Store{Client: clients.effects.Dynamic}, HostRoot: fixture.root, RPCCertificate: "certificate", VerifyPool: func(string, volumeapi.Pool) error { return nil }}
			service := &rpcserver.Service{Identity: fixture.identity, Executor: node, Pools: registry, VerifyIdentity: node.VerifyRPC,
				Probe: &measurement.Probe{Pools: registry, HostRoot: fixture.root, VerifyMount: func(string, volumeapi.Pool) error { return nil },
					Read: func(string) (capacity.Filesystem, error) {
						return capacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30, AvailableInodes: 1 << 20}, nil
					}}}
			authorizer := security.Authorizer{Client: clients.effects.Typed, Namespace: fixture.identity.Namespace, ServiceAccount: "controller"}
			caller := metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer fixture-token"))
			budget.reset(100)
			fixture.resetCounts()
			gates := make([]chan struct{}, streams)
			for i := range gates {
				gates[i] = make(chan struct{}, 1)
			}
			results := make(chan error, len(fixture.copies))
			for i, copy := range fixture.copies {
				go func(i int, copy volume.CopyIdentity) {
					pool := pools[i%2]
					evidence, err := capacity.MeasurementEvidence(pool)
					if err != nil {
						results <- err
						return
					}
					gate := gates[i%streams]
					select {
					case gate <- struct{}{}:
					case <-ctx.Done():
						results <- ctx.Err()
						return
					}
					_, err = authorizer.Unary(caller, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
						return service.GetCapacity(ctx, &protocol.CapacityRequest{ExecutorUid: fixture.identity.PodUID, NodeName: copy.NodeName, PoolName: copy.PoolName, PoolUid: copy.PoolUID, Evidence: evidence, Nonce: strings.Repeat("0", 32)})
					})
					<-gate
					if err == nil {
						_, err = authorizer.Unary(caller, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
							return service.CreateCopy(ctx, &protocol.EffectRequest{ExecutorUid: fixture.identity.PodUID, NodeName: copy.NodeName, VolumeName: copy.VolumeID, VolumeUid: copy.VolumeUID, OperationId: "create-" + copy.VolumeUID})
						})
					}
					results <- err
				}(i, copy)
			}
			select {
			case <-budget.blocked:
			case <-ctx.Done():
				t.Fatal("creation never exhausted its shared effects budget")
			}
			calls, receipts := fixture.counts()
			if calls > 100 || receipts == len(fixture.copies) {
				t.Fatalf("creation bypassed exhausted budget: API calls=%d receipts=%d", calls, receipts)
			}
			// Allow a bounded remaining request envelope, including live authority
			// and receipts. This catches a polling leak without mirroring each GET.
			budget.release(40*len(fixture.copies) - 100)
			for range fixture.copies {
				select {
				case err := <-results:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("creation did not converge after budget release")
				}
			}
			calls, receipts = fixture.counts()
			if receipts != len(fixture.copies) || calls > 40*len(fixture.copies) || int64(calls) != budget.used.Load() {
				t.Fatalf("budget/receipt accounting: API calls=%d consumed=%d receipts=%d", calls, budget.used.Load(), receipts)
			}
			for _, copy := range fixture.copies {
				state, err := registry.Get(ctx, copy.VolumeID)
				if err != nil || !volumeapi.ValidCreationReceipt(state) || *state.CurrentCopy != copy || state.Phase != volumeapi.PhaseNodeCreating {
					t.Fatalf("creation changed durable authority: state=%+v error=%v", state, err)
				}
				root := filepath.Join(fixture.root, copy.PoolName)
				if err := ownership.VerifyServingPath(root, copy); err != nil {
					t.Fatalf("missing exact serving copy: %v", err)
				}
				other := "pool-0"
				if copy.PoolName == other {
					other = "pool-1"
				}
				if _, err := os.Stat(filepath.Join(fixture.root, other, "volumes", copy.VolumeID)); !os.IsNotExist(err) {
					t.Fatalf("copy leaked into other Pool: %v", err)
				}
			}
			t.Logf("%d admission streams, 2 Pools, 20 exact receipts, %d budgeted API requests", streams, calls)
		})
	}
}

type creationFixture struct {
	identity             volumeapi.NodeExecutor
	root                 string
	copies               []volume.CopyIdentity
	server               *httptest.Server
	mu                   sync.Mutex
	objects              map[string]map[string]any
	pools                map[string]map[string]any
	calls, receiptWrites int
}

func newCreationFixture(t *testing.T) *creationFixture {
	t.Helper()
	f := &creationFixture{identity: volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "pod-uid", NodeName: "worker-a"}, root: t.TempDir(), objects: map[string]map[string]any{}, pools: map[string]map[string]any{}}
	now := metav1.Now()
	for i := range 2 {
		name := fmt.Sprintf("pool-%d", i)
		unit := &volumeapi.PoolCapacityUnit{Device: fmt.Sprintf("8:%d", i+1), Source: "/dev/" + name, Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: uint64(i) * (1 << 21), Sectors: 1 << 21}}}
		f.pools[name] = map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": name, "uid": name + "-uid", "generation": int64(1), "finalizers": []string{volumeapi.PoolProtectionFinalizer}},
			"spec": map[string]any{"nodeName": f.identity.NodeName, "poolGroup": "default", "mountPath": "/" + name, "capacityPolicy": volumeapi.PoolCapacityPolicyFixedBlock, "capacity": map[string]any{"limit": "1Gi"}},
			"status": volumeapi.PoolStatus{RegistrationApproved: true, ObservedGeneration: 1, LastProbeTime: now, CapacityUnit: unit, Inventory: &volumeapi.PoolInventory{Valid: true, ObservedAt: now}, Conditions: []metav1.Condition{
				{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: now, Reason: "PoolReady"},
				{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: now, Reason: "CapacityAllocationVerified"}}}}
		root := filepath.Join(f.root, name)
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		store, err := ownership.Open(root, ownership.PoolIdentity{InstallationID: "installation", PoolUID: name + "-uid"})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 {
		id := fmt.Sprintf("shiftpv-%032x", i+1)
		name := fmt.Sprintf("pool-%d", i%2)
		copy := volume.CopyIdentity{InstallationID: "installation", PoolName: name, PoolUID: name + "-uid", VolumeID: id, VolumeUID: id + "-uid", CopyID: id + "-copy", NodeName: f.identity.NodeName, Role: volume.RoleServing}
		f.copies = append(f.copies, copy)
		f.objects[id] = map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": id, "uid": copy.VolumeUID, "resourceVersion": "1", "finalizers": []string{volumeapi.VolumeProtectionFinalizer}},
			"spec":   map[string]any{"volumeID": id, "requestName": "pvc-" + id, "initialNode": f.identity.NodeName, "capacityBytes": int64(1024)},
			"status": map[string]any{"phase": volumeapi.PhaseNodeCreating, "ownerNode": f.identity.NodeName, "creationOperationID": "create-" + copy.VolumeUID, "creationExecutor": f.identity, "currentCopy": copy}}
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

func (f *creationFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	w.Header().Set("Content-Type", "application/json")
	var response any
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tokenreviews"):
		response = map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "status": map[string]any{"authenticated": true, "audiences": []string{security.Audience}, "user": map[string]any{"username": "system:serviceaccount:shiftpv:controller", "uid": "controller-uid"}}}
	case r.URL.Path == "/api/v1/namespaces/shiftpv/pods/node-a":
		response = map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": f.identity.PodName, "namespace": f.identity.Namespace, "uid": f.identity.PodUID, "annotations": map[string]any{security.CertificateAnnotation: "certificate"}}, "spec": map[string]any{"nodeName": f.identity.NodeName}}
	case r.URL.Path == "/api/v1/namespaces/kube-system":
		response = map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": "installation"}}
	case strings.HasSuffix(r.URL.Path, "/shiftpvpools"):
		response = map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPoolList", "items": []any{f.pools["pool-0"], f.pools["pool-1"]}}
	case strings.Contains(r.URL.Path, "/shiftpvpools/"):
		response = f.pools[strings.TrimPrefix(r.URL.Path, "/apis/shiftpv.io/v1alpha1/shiftpvpools/")]
	case strings.Contains(r.URL.Path, "/shiftpvvolumes/"):
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/apis/shiftpv.io/v1alpha1/shiftpvvolumes/"), "/")
		response = f.objects[parts[0]]
		if r.Method == http.MethodPut && len(parts) == 2 && parts[1] == "status" {
			var object map[string]any
			if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			current := f.objects[parts[0]]["metadata"].(map[string]any)
			next := object["metadata"].(map[string]any)
			if next["uid"] != current["uid"] || next["resourceVersion"] != current["resourceVersion"] {
				w.WriteHeader(http.StatusConflict)
				return
			}
			version, _ := strconv.Atoi(current["resourceVersion"].(string))
			next["resourceVersion"] = strconv.Itoa(version + 1)
			f.objects[parts[0]], response = object, object
			f.receiptWrites++
		}
	default:
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (f *creationFixture) resetCounts() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls, f.receiptWrites = 0, 0
}

func (f *creationFixture) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.receiptWrites
}
