//go:build linux || darwin

package metadata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type fakeRepository struct {
	pools                      []volumeapi.Pool
	moves                      []volumeapi.Move
	exists                     bool
	getErr, movesErr, poolsErr error
	onGet                      func()
}

func (r *fakeRepository) InstallationID(context.Context) (string, error) { return "installation", nil }
func (r *fakeRepository) ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error) {
	return r.pools, r.poolsErr
}
func (r *fakeRepository) Get(context.Context, string) (volumeapi.State, error) {
	if r.onGet != nil {
		r.onGet()
	}
	if r.getErr != nil {
		return volumeapi.State{}, r.getErr
	}
	if r.exists {
		return volumeapi.State{}, nil
	}
	return volumeapi.State{}, apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, "volume")
}
func (r *fakeRepository) ListMoves(context.Context) ([]volumeapi.Move, error) {
	return r.moves, r.movesErr
}

func readyPool(now time.Time) volumeapi.Pool {
	return volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "worker-a", MountPath: "/storage", Generation: 1, Status: volumeapi.PoolStatus{ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now), Inventory: &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}, Conditions: []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
}

func TestCollectorRequiresLiveUnreferencedReadyPool(t *testing.T) {
	for _, name := range []string{"unreferenced", "Volume exists", "Move exists", "Get unavailable", "Move list unavailable", "Pool changed during authority", "inventory invalidated during authority", "mount unavailable", "foreign node", "historical Pool exists", "stale inventory", "truncated inventory", "future inventory", "relative path", "root path", "invalid mount policy", "invalid capacity policy"} {
		t.Run(name, func(t *testing.T) {
			now := time.Now().UTC()
			repo := &fakeRepository{pools: []volumeapi.Pool{readyPool(now)}}
			target := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "old-volume", CopyID: "old-copy", NodeName: "worker-a", Role: volume.RoleServing}
			verify := func(context.Context, volumeapi.Pool) error { return nil }
			switch name {
			case "Volume exists":
				repo.exists = true
			case "Move exists":
				repo.moves = []volumeapi.Move{{Spec: volumeapi.MoveSpec{VolumeID: target.VolumeID}}}
			case "Get unavailable":
				repo.getErr = errors.New("GET failed")
			case "Move list unavailable":
				repo.movesErr = errors.New("LIST failed")
			case "Pool changed during authority":
				repo.onGet = func() { repo.pools[0].Generation++ }
			case "inventory invalidated during authority":
				repo.onGet = func() { repo.pools[0].Status.Inventory.Valid = false }
			case "mount unavailable":
				verify = func(context.Context, volumeapi.Pool) error { return errors.New("backing changed") }
			case "foreign node":
				target.NodeName = "worker-b"
			case "historical Pool exists":
				target.PoolUID = "old-pool"
				repo.pools = append(repo.pools, volumeapi.Pool{UID: "old-pool", NodeName: "worker-b"})
			case "stale inventory":
				repo.pools[0].Status.Inventory.ObservedAt = metav1.NewTime(now.Add(-time.Hour))
			case "truncated inventory":
				repo.pools[0].Status.Inventory.Truncated = true
			case "future inventory":
				repo.pools[0].Status.Inventory.ObservedAt = metav1.NewTime(now.Add(time.Hour))
			case "relative path":
				repo.pools[0].MountPath = "../../storage"
			case "root path":
				repo.pools[0].MountPath = "/"
			case "invalid mount policy":
				repo.pools[0].MountPolicy = "Unknown"
			case "invalid capacity policy":
				repo.pools[0].CapacityPolicy = "Unknown"
			}
			removed := false
			collector := &Collector{NodeName: "worker-a", HostRoot: "/host", Repository: repo, Retention: DefaultRetention, Now: func() time.Time { return now }, VerifyPool: verify}
			collector.Collect = func(ctx context.Context, root string, pool ownership.PoolIdentity, observed time.Time, retention time.Duration, unused func(context.Context, volume.CopyIdentity) (bool, error)) (int, error) {
				if root != "/host/storage" || pool.PoolUID != "pool-uid" || retention != DefaultRetention {
					t.Fatal("incorrect metadata scope")
				}
				allowed, err := unused(ctx, target)
				removed = allowed && err == nil
				return 0, err
			}
			err := collector.Reconcile(context.Background())
			if name == "unreferenced" {
				if err != nil || !removed {
					t.Fatalf("eligible GC blocked: %v", err)
				}
			} else if removed {
				t.Fatal("unsafe GC admitted", name)
			}
		})
	}
}
