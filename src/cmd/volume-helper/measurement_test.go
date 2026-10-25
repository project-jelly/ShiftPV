package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type measurementRepository struct {
	pool  volumeapi.Pool
	state volumeapi.State
	err   error
}

func (r *measurementRepository) PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return r.pool, r.err
}

func (r *measurementRepository) ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return r.pool, r.err
}
func (r *measurementRepository) InventoryPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return r.pool, r.err
}
func (r *measurementRepository) Get(context.Context, string) (volumeapi.State, error) {
	return r.state, r.err
}
func (r *measurementRepository) InstallationID(context.Context) (string, error) {
	return "installation", r.err
}

func measurementFixture(t *testing.T) (*measurementRepository, measurementOptions) {
	t.Helper()
	pool := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/mnt/a", Generation: 1}
	now := time.Now()
	pool.Finalizers = []string{volumeapi.PoolProtectionFinalizer}
	pool.Status = volumeapi.PoolStatus{ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now),
		Inventory: &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}}
	for _, kind := range []string{volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable} {
		pool.Status.Conditions = append(pool.Status.Conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, ObservedGeneration: 1})
	}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: pool.Name, PoolUID: pool.UID, NodeName: pool.NodeName,
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", Role: volume.RoleServing}
	evidence, err := capacity.MeasurementEvidence(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &measurementRepository{pool: pool, state: volumeapi.State{UID: copy.VolumeUID, OwnerNode: copy.NodeName, Phase: volumeapi.PhaseMoving, CurrentCopy: &copy}},
		measurementOptions{pool: pool, root: t.TempDir(), evidence: evidence, volumeID: copy.VolumeID, copy: &copy, now: func() time.Time { return now }}
}

func TestMeasurementRejectsContradictionsBeforeReading(t *testing.T) {
	for _, scenario := range []string{"API error", "generation", "mount", "copy marker", "copy identity", "published", "installation"} {
		t.Run(scenario, func(t *testing.T) {
			repo, options := measurementFixture(t)
			verifyMount := func(string, volumeapi.Pool) error { return nil }
			verifyCopy := func(string, volume.CopyIdentity) error { return nil }
			switch scenario {
			case "API error":
				repo.err = errors.New("API unavailable")
			case "generation":
				repo.pool.Generation++
			case "mount":
				verifyMount = func(string, volumeapi.Pool) error { return errors.New("mount changed") }
			case "copy marker":
				verifyCopy = func(string, volume.CopyIdentity) error { return errors.New("marker mismatch") }
			case "copy identity":
				copy := *repo.state.CurrentCopy
				copy.CopyID = "replacement-copy"
				repo.state.CurrentCopy = &copy
			case "published":
				repo.state.PublishedNodes = []string{repo.pool.NodeName}
			case "installation":
				copy := *repo.state.CurrentCopy
				copy.InstallationID = "replacement"
				repo.state.CurrentCopy = &copy
				options.copy = nil
			}
			reads := 0
			output, err := measureWithAuthority(context.Background(), repo, options, verifyMount, verifyCopy, func(context.Context) (string, error) { reads++; return "123", nil })
			if err == nil || output != "" || reads != 0 {
				t.Fatalf("output=%q error=%v reads=%d", output, err, reads)
			}
		})
	}
}

func TestMeasurementDiscardsResultWhenAuthorityChangesDuringRead(t *testing.T) {
	for _, scenario := range []string{"Pool", "mount", "copy", "published", "read error"} {
		t.Run(scenario, func(t *testing.T) {
			repo, options := measurementFixture(t)
			options.copy = nil // also exercises pinning for the legacy usage entry point
			mountChecks := 0
			verifyMount := func(string, volumeapi.Pool) error {
				mountChecks++
				if scenario == "mount" && mountChecks == 2 {
					return errors.New("mount disappeared")
				}
				return nil
			}
			output, err := measureWithAuthority(context.Background(), repo, options, verifyMount, func(string, volume.CopyIdentity) error { return nil }, func(context.Context) (string, error) {
				switch scenario {
				case "Pool":
					repo.pool.PoolGroup = "changed-group"
				case "copy":
					copy := *repo.state.CurrentCopy
					copy.CopyID = "replacement-copy"
					repo.state.CurrentCopy = &copy
				case "published":
					repo.state.PublishedNodes = []string{repo.pool.NodeName}
				case "read error":
					return "", errors.New("du failed")
				}
				return "123", nil
			})
			if err == nil || output != "" {
				t.Fatalf("changed authority result accepted: output=%q error=%v", output, err)
			}
		})
	}
}

func TestMeasurementChecksMountAndCopyBeforeAndAfterRead(t *testing.T) {
	repo, options := measurementFixture(t)
	mounts, copies := 0, 0
	output, err := measureWithAuthority(context.Background(), repo, options,
		func(string, volumeapi.Pool) error { mounts++; return nil },
		func(string, volume.CopyIdentity) error { copies++; return nil },
		func(context.Context) (string, error) { return "123", nil })
	if err != nil || output != "123" || mounts != 2 || copies != 2 {
		t.Fatalf("output=%q error=%v mounts=%d copies=%d", output, err, mounts, copies)
	}
}

func TestMeasurementRegistrySeparatesExistingUsageFromNewAllocation(t *testing.T) {
	for _, action := range []string{"usage", "statfs"} {
		t.Run(action, func(t *testing.T) {
			registry, _, options := measurementRegistryFixture(t)
			if action == "statfs" {
				options.volumeID, options.copy = "", nil
			}
			mounts, copies, reads := 0, 0, 0
			output, err := measureWithAuthority(context.Background(), registry, options,
				func(string, volumeapi.Pool) error { mounts++; return nil },
				func(string, volume.CopyIdentity) error { copies++; return nil },
				func(context.Context) (string, error) { reads++; return "123", nil })
			if action == "usage" {
				if err != nil || output != "123" || reads != 1 || mounts != 2 || copies != 2 {
					t.Fatalf("existing usage blocked by peer: output=%q err=%v reads=%d mounts=%d copies=%d", output, err, reads, mounts, copies)
				}
			} else if err == nil || output != "" || reads != 0 || mounts != 0 || copies != 0 {
				t.Fatalf("unproven allocation measured: output=%q err=%v reads=%d", output, err, reads)
			}
		})
	}
}

func TestMeasurementRegistryRejectsChangedUsageProofBeforeAndAfterRead(t *testing.T) {
	for _, afterRead := range []bool{false, true} {
		for _, scenario := range []string{"own backing", "stale inventory", "Pool UID", "generation", "evidence", "copy", "published"} {
			name := "before/" + scenario
			if afterRead {
				name = "after/" + scenario
			}
			t.Run(name, func(t *testing.T) {
				registry, client, options := measurementRegistryFixture(t)
				// Generation/time-only staleness waits without publishing the du
				// result. Bound that wait here; dedicated tests refresh the proof.
				options.wait = func(context.Context) error { return context.DeadlineExceeded }
				change := func() {
					resource, objectName := volumeapi.PoolResource, options.pool.Name
					if scenario == "copy" || scenario == "published" {
						resource, objectName = volumeapi.VolumeResource, options.volumeID
					}
					object, err := client.Resource(resource).Get(context.Background(), objectName, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					switch scenario {
					case "own backing":
						conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
						for _, item := range conditions {
							condition := item.(map[string]any)
							if condition["type"] == volumeapi.PoolConditionCapacityBackingVerified {
								condition["status"] = "False"
							}
						}
						_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
					case "stale inventory":
						_ = unstructured.SetNestedField(object.Object, registry.Now().Add(-2*time.Minute).Format(time.RFC3339), "status", "inventory", "observedAt")
					case "Pool UID":
						object.SetUID("replacement")
					case "generation":
						object.SetGeneration(2)
					case "evidence":
						_ = unstructured.SetNestedField(object.Object, "/dev/replaced", "status", "capacityUnit", "source")
					case "copy":
						_ = unstructured.SetNestedField(object.Object, "replacement-copy", "status", "currentCopy", "copyID")
					case "published":
						_ = unstructured.SetNestedStringSlice(object.Object, []string{options.pool.NodeName}, "status", "publishedNodes")
					}
					if _, err := client.Resource(resource).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				if !afterRead {
					change()
				}
				reads := 0
				output, err := measureWithAuthority(context.Background(), registry, options,
					func(string, volumeapi.Pool) error { return nil }, func(string, volume.CopyIdentity) error { return nil },
					func(context.Context) (string, error) { reads++; change(); return "123", nil })
				wantReads := 0
				if afterRead {
					wantReads = 1
				}
				if err == nil || output != "" || reads != wantReads {
					t.Fatalf("changed usage proof accepted: output=%q err=%v reads=%d want=%d", output, err, reads, wantReads)
				}
			})
		}
	}
}

func measurementRegistryFixture(t *testing.T) (*volumeapi.Registry, *dynamicfake.FakeDynamicClient, measurementOptions) {
	t.Helper()
	repo, options := measurementFixture(t)
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	pool := repo.pool
	pool.PoolGroup, pool.CapacityPolicy, pool.CapacityLimit = volumeapi.DefaultPoolGroup, volumeapi.PoolCapacityPolicyFixedBlock, "1Gi"
	pool.Status = volumeapi.PoolStatus{
		RegistrationApproved: true, ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now),
		CapacityUnit: &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/a", Filesystem: "ext4",
			Extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}},
		Inventory: &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now),
			Copies: []volumeapi.CopyObservation{{Identity: options.copy, Present: true}}},
	}
	for _, kind := range []string{volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable, volumeapi.PoolConditionCapacityBackingVerified} {
		pool.Status.Conditions = append(pool.Status.Conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, ObservedGeneration: 1})
	}
	for _, kind := range []string{volumeapi.PoolConditionReady, volumeapi.PoolConditionCapacityIndependent} {
		pool.Status.Conditions = append(pool.Status.Conditions, metav1.Condition{Type: kind, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: "CapacityPeerUnproven"})
	}
	status, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(&pool.Status)
	if err != nil {
		t.Fatal(err)
	}
	copy, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(options.copy)
	if err != nil {
		t.Fatal(err)
	}
	objects := []k8sruntime.Object{
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
			"metadata": map[string]any{"name": pool.Name, "uid": pool.UID, "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
			"spec": map[string]any{"nodeName": pool.NodeName, "poolGroup": pool.PoolGroup, "mountPath": pool.MountPath,
				"capacityPolicy": pool.CapacityPolicy, "capacity": map[string]any{"limit": pool.CapacityLimit}},
			"status": status,
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
			"metadata": map[string]any{"name": options.volumeID, "uid": options.copy.VolumeUID},
			"spec":     map[string]any{"volumeID": options.volumeID},
			"status":   map[string]any{"phase": volumeapi.PhaseMoving, "ownerNode": pool.NodeName, "activeMove": "move", "currentCopy": copy},
		}},
		&unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": options.copy.InstallationID}}},
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.PoolResource: "ShiftPVPoolList", volumeapi.VolumeResource: "ShiftPVVolumeList",
	}, objects...)
	registry := &volumeapi.Registry{Client: client, Now: func() time.Time { return now }, PoolReadinessStaleAfter: time.Minute}
	options.pool = pool
	options.now, options.staleAfter = registry.Now, registry.PoolReadinessStaleAfter
	options.evidence, err = capacity.MeasurementEvidence(pool)
	if err != nil {
		t.Fatal(err)
	}
	return registry, client, options
}

func TestParseMeasurementRequiresBoundPoolAndCopy(t *testing.T) {
	_, options := measurementFixture(t)
	args := []string{"--pool-name=" + options.pool.Name, "--pool-uid=" + options.pool.UID, "--node-name=" + options.pool.NodeName, "--pool-evidence=" + options.evidence}
	if _, err := parseMeasurementOptions("statfs", args); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMeasurementOptions("statfs", args[:3]); err == nil {
		t.Fatal("missing evidence accepted")
	}
	if _, err := parseMeasurementOptions("usage", append(append([]string{}, args...), "--volume-id=../escape")); err == nil {
		t.Fatal("unsafe volume accepted")
	}
	copy := *options.copy
	copy.PoolUID = "other-uid"
	encoded, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseMeasurementOptions("usage", append(args, "--volume-id="+copy.VolumeID, "--copy-identity="+string(encoded))); err == nil {
		t.Fatal("copy in another Pool accepted")
	}
}

func TestReadMeasurementStatFS(t *testing.T) {
	_, options := measurementFixture(t)
	output, err := readMeasurement(context.Background(), "statfs", options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capacity.ParseStatOutput(output); err != nil {
		t.Fatal(err)
	}
}

func TestReadMeasurementUsagePropagatesDUFailure(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("runtime uses Linux du")
	}
	_, options := measurementFixture(t)
	if _, err := readMeasurement(context.Background(), "usage", options); err == nil {
		t.Fatal("missing serving directory was measured successfully")
	}
	path, err := volume.Path(options.root, options.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "data"), []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := readMeasurement(context.Background(), "usage", options)
	if err != nil || strings.TrimSpace(output) == "" {
		t.Fatalf("output=%q error=%v", output, err)
	}
}
