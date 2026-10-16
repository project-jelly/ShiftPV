package realnode_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const testVolumeID = "shiftpv-0123456789abcdef0123456789abcdef"
const testNodeImage = "example.invalid/shiftpv@sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type soakFixture struct {
	dir   string
	move  *unstructured.Unstructured
	pod   corev1.Pod
	ds    appsv1.DaemonSet
	clean cleanupapi.Cleanup
}

// Use the actual Registry status writer and typed cleanup payload. In
// particular, ClaimUID's JSON spelling must not be invented by the shell TC.
func newSoakFixture(t *testing.T) *soakFixture {
	t.Helper()
	f := &soakFixture{dir: t.TempDir()}
	initial := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move", "uid": "move-uid", "resourceVersion": "1",
			"finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":   map[string]any{"protocolVersion": volumeapi.MoveProtocolPreparedV1, "volumeID": testVolumeID, "sourceNode": "source"},
		"status": map[string]any{"phase": "Pending"},
	}}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), initial)
	registry := &volumeapi.Registry{Client: client}
	moves, err := registry.ListMoves(context.Background())
	if err != nil || len(moves) != 1 {
		t.Fatalf("read fixture Move: %v (%v)", err, moves)
	}
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: testVolumeID, VolumeUID: "volume-uid", NodeName: "source", CopyID: "source-copy", Role: volume.RoleServing}
	incoming, destination := volumeapi.MoveTransactionCopies("move-uid", testVolumeID, volumeapi.MoveDestinationAnchor{
		InstallationID: "installation", PoolName: "destination-pool", PoolUID: "destination-pool-uid", NodeName: "destination", VolumeUID: "volume-uid",
	})
	status := volumeapi.MoveStatus{Phase: "CleaningSource", PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid",
		ClaimNamespace: "test", ClaimName: "data", ClaimUID: "claim-uid", ConsumerName: "writer", ConsumerUID: "writer-uid",
		PlacementPodUID: "placement-uid", DestinationNode: "destination", DestinationPoolUID: "destination-pool-uid",
		CapacityApproved: true, SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
		CopyOperationID: volumeapi.MoveCopyOperationID("move-uid"), PromotionOperationID: volumeapi.MovePromotionOperationID("move-uid")}
	preparing := status
	preparing.Phase, preparing.CapacityApproved = "WaitingForCapacity", false
	preparing.IncomingCopy, preparing.DestinationCopy = nil, nil
	preparing.CopyOperationID, preparing.PromotionOperationID = "", ""
	preparing.Preparation = &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: "destination-pool", UID: "destination-pool-uid"}, NodeName: "destination", ReservedBytes: 1024}
	prepared, err := registry.CompareAndSetMoveStatus(context.Background(), moves[0], preparing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.CompareAndSetMoveStatus(context.Background(), prepared, status); err != nil {
		t.Fatal(err)
	}
	f.move, err = client.Resource(volumeapi.MoveResource).Get(context.Background(), "move", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.clean = cleanupapi.Cleanup{UID: "move-uid", Spec: cleanupapi.Spec{OperationID: volumeapi.MoveCleanupOperationID("move-uid"),
		Target: source, Reason: "MoveSource", Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move", UID: "move-uid"}},
		Status: cleanupapi.Status{Phase: "Running", Executor: &cleanupapi.Executor{Kind: "Node", Namespace: "system",
			PodName: "resident", PodUID: "old-pod-uid", NodeName: "source"}}}
	controller := true
	f.ds = appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "shiftpv-node", Namespace: "system", UID: "daemonset-uid"},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: "node",
			Containers: []corev1.Container{{Name: "shiftpv-node", Image: testNodeImage}}}}}}
	f.pod = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "resident", Namespace: "system", UID: "old-pod-uid",
		Annotations:     map[string]string{volumeapi.NodeEffectsAnnotation: "v1", volumeapi.NodeMoveCleanupAnnotation: "v1"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: f.ds.Name, UID: f.ds.UID, Controller: &controller}}},
		Spec:   corev1.PodSpec{NodeName: "source", ServiceAccountName: "node", Containers: []corev1.Container{{Name: "shiftpv-node", Image: testNodeImage}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	f.save(t)
	return f
}

func writeJSON(t *testing.T, path string, object any) {
	t.Helper()
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *soakFixture) save(t *testing.T) {
	t.Helper()
	journal, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&struct {
		Spec   cleanupapi.Spec   `json:"spec"`
		Status cleanupapi.Status `json:"status"`
	}{f.clean.Spec, f.clean.Status})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedMap(f.move.Object, journal, "status", "cleanup"); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(f.dir, "move.json"), f.move.Object)
	writeJSON(t, filepath.Join(f.dir, "pod.json"), f.pod)
	writeJSON(t, filepath.Join(f.dir, "ds.json"), f.ds)
}

const kubectlStub = `#!/usr/bin/env bash
set -euo pipefail
test "$1" = --context && test "$2" = reviewed && test "$3" = --request-timeout=30s
shift 3
printf '%s\n' "$*" >>"${FIXTURE}/calls"
if [[ "$1" == -n ]]; then test "$2" = system; shift 2; fi
case "$1 $2" in
 'wait shiftpvmove/move') exit 0 ;;
 'get pod/resident') cat "${FIXTURE}/pod.json" ;;
 'get daemonset/shiftpv-node') cat "${FIXTURE}/ds.json" ;;
 'get deployment/shiftpv-controller')
   if [[ "$4" == jsonpath=* ]]; then printf controller-deployment-uid; else cat "${FIXTURE}/controller-deployment.json"; fi ;;
 'get replicaset/controller-rs') cat "${FIXTURE}/controller-rs.json" ;;
 'get pod') jq -n --slurpfile pod "${FIXTURE}/controller-pod.json" '{items:$pod}' ;;
 'get pod/controller-pod') cat "${FIXTURE}/controller-pod.json" ;;
 'rollout status') test "$3" = deployment/shiftpv-controller ;;
 'get shiftpvmove/move') cat "${FIXTURE}/move.json" ;;
 'get shiftpvvolume/'*) cat "${FIXTURE}/volume.json" ;;
 'get shiftpvpool/source-pool') cat "${FIXTURE}/pool.json" ;;
 'get shiftpvpools,shiftpvvolumes,shiftpvmoves') echo '{"items":[]}' ;;
 'uncordon source'|'uncordon destination') test "${STUB_UNCORDON_FAIL:-0}" = 0 ;;
 'replace --subresource=status') cat >/dev/null; echo 'real-node cleanup admission barrier' >&2; exit 1 ;;
 'delete --raw')
   case "$3" in
    /api/v1/namespaces/system/pods/resident) pod=pod; uid=old-pod-uid; replacement=new-pod-uid ;;
    /api/v1/namespaces/system/pods/controller-pod) pod=controller-pod; uid=old-controller-uid; replacement=new-controller-uid ;;
    *) exit 90 ;;
   esac
   cat >"${FIXTURE}/delete-options.json"
   jq -e --arg uid "$uid" '.kind == "DeleteOptions" and .preconditions.uid == $uid' "${FIXTURE}/delete-options.json" >/dev/null
   jq --arg uid "$replacement" '.metadata.uid = $uid' "${FIXTURE}/${pod}.json" >"${FIXTURE}/replacement.json"
   mv "${FIXTURE}/replacement.json" "${FIXTURE}/${pod}.json"
   echo 'accepted DELETE response lost' >&2; exit 1 ;;
 *) echo "unexpected kubectl: $*" >&2; exit 90 ;;
esac
`

func (f *soakFixture) run(t *testing.T, command string) (string, error) {
	t.Helper()
	for _, name := range []string{"bash", "jq"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skipf("local shell contract tests require %s", name)
		}
	}
	bin := filepath.Join(f.dir, "bin")
	if err := os.MkdirAll(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{"kubectl": kubectlStub, "ssh": `#!/usr/bin/env bash
echo SSH >>"${FIXTURE}/calls"
if [[ ${STUB_SSH_SLEEP:-0} == 1 ]]; then
 printf '%s\n' "$$" >"${FIXTURE}/ssh-pid"
 exec sleep 30
fi
exit 91
`} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", `set -euo pipefail
source "$SCRIPTS/lib.sh"
source "$SCRIPTS/cleanup-executor.sh"
`+command)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"SCRIPTS="+dir, "FIXTURE="+f.dir, "ARTIFACT_DIR="+f.dir, "KUBECTL_CONTEXT=reviewed",
		"SOURCE_NODE=source", "DESTINATION_NODE=destination", "SOURCE_POOL=/source", "DESTINATION_POOL=/destination",
		"SOURCE_SSH_TARGET=reviewed-source", "DESTINATION_SSH_TARGET=reviewed-destination", "SSH_CONNECT_TIMEOUT=1",
		"SYSTEM_NAMESPACE=system", "NODE_DAEMONSET=shiftpv-node", "MOVE_TIMEOUT_SECONDS=3", "EXPECTED_NODE_IMAGE="+testNodeImage,
		"CONTROLLER_DEPLOYMENT=shiftpv-controller", "EXPECTED_CONTROLLER_IMAGE="+testNodeImage,
		"VOLUME_ID="+testVolumeID, "VOLUME_UID=volume-uid", "PV_UID=pv-uid", "PVC_UID=claim-uid")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func (f *soakFixture) noEffects(t *testing.T) {
	t.Helper()
	calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
	if strings.Contains(string(calls), "SSH") || strings.Contains(string(calls), "delete ") {
		t.Fatalf("rejected identity reached an effect: %s", calls)
	}
}

func TestNativeCleanupRejectsIdentityBeforeEffects(t *testing.T) {
	for name, mutate := range map[string]func(*soakFixture){
		"source node":     func(f *soakFixture) { f.clean.Status.Executor.NodeName = "destination" },
		"wrong operation": func(f *soakFixture) { f.clean.Spec.OperationID = "cleanup-another-move" },
		"wrong authority": func(f *soakFixture) { f.clean.Spec.Authority.UID = "another-move" },
		"wrong target":    func(f *soakFixture) { f.clean.Spec.Target.CopyID = "another-copy" },
		"wrong volume UID": func(f *soakFixture) {
			_ = unstructured.SetNestedField(f.move.Object, "another-volume", "status", "destinationCopy", "volumeUID")
		},
		"wrong Pod UID":               func(f *soakFixture) { f.clean.Status.Executor.PodUID = "another-pod" },
		"live Pod node":               func(f *soakFixture) { f.pod.Spec.NodeName = "destination" },
		"live Pod DaemonSet":          func(f *soakFixture) { f.pod.OwnerReferences[0].UID = "another-daemonset" },
		"live Pod image":              func(f *soakFixture) { f.pod.Spec.Containers[0].Image = "another-image" },
		"missing recovery capability": func(f *soakFixture) { delete(f.pod.Annotations, volumeapi.NodeMoveCleanupAnnotation) },
		"receipt already exists":      func(f *soakFixture) { f.clean.Status.Receipt = &cleanupapi.Receipt{ExecutorUID: "old-pod-uid"} },
	} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			mutate(f)
			f.save(t)
			if out, err := f.run(t, `delete_bound_cleanup_pod "$FIXTURE/move.json" source`); err == nil {
				t.Fatalf("invalid identity accepted: %s", out)
			}
			f.noEffects(t)
		})
	}
	t.Run("unreviewed role", func(t *testing.T) {
		f := newSoakFixture(t)
		if out, err := f.run(t, `delete_bound_cleanup_pod "$FIXTURE/move.json" unreviewed`); err == nil {
			t.Fatalf("unreviewed role accepted: %s", out)
		}
		f.noEffects(t)
	})
}

func TestNativeCleanupAcceptedDeleteLostResponseDoesNotDeleteReplacement(t *testing.T) {
	f := newSoakFixture(t)
	if out, err := f.run(t, `delete_bound_cleanup_pod "$FIXTURE/move.json" source`); err != nil {
		t.Fatalf("accepted deletion was not re-observed: %v\n%s", err, out)
	}
	calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
	if strings.Count(string(calls), "delete --raw ") != 1 || strings.Contains(string(calls), "SSH") {
		t.Fatalf("DELETE must be sent once, with no remote command: %s", calls)
	}
	var pod corev1.Pod
	data, _ := os.ReadFile(filepath.Join(f.dir, "pod.json"))
	if err := json.Unmarshal(data, &pod); err != nil || pod.UID != "new-pod-uid" {
		t.Fatalf("same-name replacement was not preserved: %v %s", err, data)
	}
}

func (f *soakFixture) rebound(t *testing.T) {
	t.Helper()
	writeJSON(t, filepath.Join(f.dir, "before.json"), f.move.Object)
	f.clean.Status.Executor.PodUID = "new-pod-uid"
	f.pod.UID = types.UID("new-pod-uid")
	f.save(t)
	writeJSON(t, filepath.Join(f.dir, "rebound.json"), f.move.Object)
}

func (f *soakFixture) completed(t *testing.T) {
	t.Helper()
	f.clean.Status.Phase = "Completed"
	f.clean.Status.Receipt = &cleanupapi.Receipt{OperationID: f.clean.Spec.OperationID, ExecutorUID: "new-pod-uid",
		Retired: true, Purged: true, LocalReceiptDigest: strings.Repeat("a", 64), ObservedAt: "2026-10-16T00:00:00Z"}
	f.clean.Status.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "scan-request", PoolName: "source-pool", PoolUID: "source-pool-uid",
		RequiredGeneration: 3, ObservedGeneration: 4, Valid: true, Complete: true, Absent: true, ConfirmedAt: "2026-10-16T00:00:00Z"}
	f.clean.Status.SettledAt = "2026-10-16T00:00:01Z"
	if err := cleanupapi.ValidateSettled(f.clean); err != nil {
		t.Fatalf("positive native proof must satisfy production validator: %v", err)
	}
	f.move.SetFinalizers(nil)
	_ = unstructured.SetNestedField(f.move.Object, "Succeeded", "status", "phase")
	f.save(t)
}

func TestNativeCleanupRebindPreservesIntentAndRequiresNewUID(t *testing.T) {
	for name, mutate := range map[string]func(*soakFixture){
		"exact rebind":      func(*soakFixture) {},
		"operation changed": func(f *soakFixture) { f.clean.Spec.OperationID = "other-operation" },
		"target changed":    func(f *soakFixture) { f.clean.Spec.Target.CopyID = "other-copy" },
		"old UID retained":  func(f *soakFixture) { f.clean.Status.Executor.PodUID = "old-pod-uid"; f.pod.UID = "old-pod-uid" },
		"hold released": func(f *soakFixture) {
			_ = unstructured.SetNestedField(f.move.Object, false, "status", "capacityApproved")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			f.rebound(t)
			mutate(f)
			f.save(t)
			out, err := f.run(t, `validate_cleanup_rebind "$FIXTURE/before.json" "$FIXTURE/move.json" source`)
			if (err == nil) != (name == "exact rebind") {
				t.Fatalf("rebind result: %v\n%s", err, out)
			}
			f.noEffects(t)
		})
	}
}

func TestNativeCleanupCompletionRequiresSameIntentReceiptAndAbsence(t *testing.T) {
	for name, mutate := range map[string]func(*soakFixture){
		"exact proof":          func(*soakFixture) {},
		"old executor receipt": func(f *soakFixture) { f.clean.Status.Receipt.ExecutorUID = "old-pod-uid" },
		"different operation": func(f *soakFixture) {
			f.clean.Spec.OperationID = "other-operation"
			f.clean.Status.Receipt.OperationID = "other-operation"
		},
		"different target":       func(f *soakFixture) { f.clean.Spec.Target.CopyID = "other-copy" },
		"wrong Pool proof":       func(f *soakFixture) { f.clean.Status.AbsenceProof.PoolUID = "other-pool" },
		"incomplete observation": func(f *soakFixture) { f.clean.Status.AbsenceProof.Complete = false },
		"stale observation":      func(f *soakFixture) { f.clean.Status.AbsenceProof.ObservedGeneration = 2 },
		"zero fence":             func(f *soakFixture) { f.clean.Status.AbsenceProof.RequiredGeneration = 0 },
		"bad digest":             func(f *soakFixture) { f.clean.Status.Receipt.LocalReceiptDigest = "bad" },
		"missing receipt time":   func(f *soakFixture) { f.clean.Status.Receipt.ObservedAt = "" },
		"unreleased finalizer":   func(f *soakFixture) { f.move.SetFinalizers([]string{volumeapi.MoveProtectionFinalizer}) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			f.rebound(t)
			f.completed(t)
			mutate(f)
			f.save(t)
			out, err := f.run(t, `validate_native_cleanup_proof "$FIXTURE/move.json" "$FIXTURE/rebound.json"`)
			if (err == nil) != (name == "exact proof") {
				t.Fatalf("completion result: %v\n%s", err, out)
			}
			f.noEffects(t)
		})
	}
}

func TestSoakQualificationRequiresDurationMovesAndSuccessfulDefaultFaults(t *testing.T) {
	for name, args := range map[string]string{
		"qualified":         "100 43200 10 20 10 5",
		"too few moves":     "99 43200 10 20 9 4",
		"too short":         "100 43199 10 20 10 5",
		"faults disabled":   "100 43200 0 0 0 0",
		"controller missed": "100 43200 10 20 9 5",
		"cleanup missed":    "100 43200 10 20 10 4",
		"changed cadence":   "100 43200 5 10 20 10",
	} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			out, err := f.run(t, "soak_result_marker "+args)
			if err != nil {
				t.Fatalf("marker: %v %s", err, out)
			}
			want := "REAL_NODE_SOAK_SMOKE_OK"
			if name == "qualified" {
				want = "REAL_NODE_SOAK_OK"
			}
			if strings.TrimSpace(out) != want {
				t.Fatalf("marker %q, want %q", out, want)
			}
			f.noEffects(t)
		})
	}
}

func TestCleanupRoleFollowsAlternatingSource(t *testing.T) {
	f := newSoakFixture(t)
	out, err := f.run(t, `cleanup_source_role source; cleanup_source_role destination`)
	if err != nil || out != "source\t/source\ndestination\t/destination\n" {
		t.Fatalf("role mapping: %v %q", err, out)
	}
	f.noEffects(t)
}

func TestNativeCleanupChecksCommittedMoveVolumeAndPoolBeforeSSH(t *testing.T) {
	for name, mutate := range map[string]func(*unstructured.Unstructured, *unstructured.Unstructured, *unstructured.Unstructured){
		"valid reaches injected SSH failure": func(*unstructured.Unstructured, *unstructured.Unstructured, *unstructured.Unstructured) {},
		"Move binding changed": func(m, _, _ *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(m.Object, "other-pv", "status", "persistentVolumeUID")
		},
		"Volume UID changed": func(_, v, _ *unstructured.Unstructured) { v.SetUID("other-volume") },
		"Volume owner changed": func(_, v, _ *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, "source", "status", "ownerNode")
		},
		"Volume claim changed": func(_, v, _ *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, "another-move", "status", "activeMove")
		},
		"Pool UID changed": func(_, _, p *unstructured.Unstructured) { p.SetUID("other-pool") },
		"Pool node changed": func(_, _, p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, "destination", "spec", "nodeName")
		},
		"Pool path changed": func(_, _, p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, "/other-pool", "spec", "mountPath")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			unstructured.RemoveNestedField(f.move.Object, "status", "cleanup")
			_ = unstructured.SetNestedField(f.move.Object, "WaitingForDestinationPublish", "status", "phase")
			destination, _, _ := unstructured.NestedMap(f.move.Object, "status", "destinationCopy")
			v := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"uid": "volume-uid"},
				"status": map[string]any{"phase": "Ready", "ownerNode": "destination", "activeMove": "move", "currentCopy": destination, "publishedNodes": []any{"destination"}}}}
			p := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"uid": "source-pool-uid"},
				"spec": map[string]any{"nodeName": "source", "mountPath": "/source"}}}
			mutate(f.move, v, p)
			writeJSON(t, filepath.Join(f.dir, "move.json"), f.move.Object)
			writeJSON(t, filepath.Join(f.dir, "volume.json"), v.Object)
			writeJSON(t, filepath.Join(f.dir, "pool.json"), p.Object)
			out, err := f.run(t, `fault_native_cleanup move source`)
			if err == nil {
				t.Fatalf("injected failure/invalid authority was accepted: %s", out)
			}
			calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
			if name == "valid reaches injected SSH failure" {
				if !strings.Contains(string(calls), "SSH") {
					t.Fatalf("valid pre-lock fixture did not reach the fault boundary: %s\n%s", calls, out)
				}
			} else {
				f.noEffects(t)
			}
		})
	}
}

func TestSoakRestorationPrecedesSuccessAndSignalsCannotSucceed(t *testing.T) {
	for _, name := range []string{"success", "uncordon failure", "capture failure", "lock release failure", "policy release failure", "validation failed before start", "INT", "TERM"} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			command := `SOAK_STARTED=1; SOAK_RESULT_READY=1
result_marker=REAL_NODE_SOAK_OK; ITERATIONS=100; elapsed_seconds=43200; controller_faults=10; cleanup_faults=5
capture_evidence() { return 0; }
install_soak_traps
`
			switch name {
			case "uncordon failure":
				t.Setenv("STUB_UNCORDON_FAIL", "1")
			case "capture failure":
				command += "capture_evidence() { return 1; }\n"
			case "lock release failure":
				command += "release_cleanup_lock() { return 1; }\n"
			case "policy release failure":
				command += "release_soak_cleanup() { return 1; }\n"
			case "validation failed before start":
				command += "SOAK_STARTED=0; SOAK_RESULT_READY=0; exit 1\n"
			case "INT", "TERM":
				command += "kill -s " + name + " $$\n"
			}
			out, err := f.run(t, command)
			if (err == nil) != (name == "success") || strings.Contains(out, "REAL_NODE_SOAK_OK") != (name == "success") {
				t.Fatalf("restore/marker result: %v\n%s", err, out)
			}
			if name == "INT" || name == "TERM" {
				code := 130
				if name == "TERM" {
					code = 143
				}
				if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != code {
					t.Fatalf("signal exit = %v, want %d", err, code)
				}
			}
			calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
			if name == "validation failed before start" && len(calls) != 0 {
				t.Fatalf("preflight failure mutated input nodes: %s", calls)
			}
		})
	}
}

func TestCleanupBackgroundSSHOwnsTheBoundedProcess(t *testing.T) {
	for _, role := range []string{"source", "destination"} {
		t.Run(role, func(t *testing.T) {
			f := newSoakFixture(t)
			t.Setenv("STUB_SSH_SLEEP", "1")
			start := time.Now()
			out, err := f.run(t, `exec_ssh_node `+role+` ignored &
pid=$!
ssh_pid=''
cleanup() {
 kill -KILL "$pid" ${ssh_pid:+"$ssh_pid"} 2>/dev/null || true
 wait "$pid" 2>/dev/null || true
}

trap cleanup EXIT
for _ in {1..100}; do
 if [[ -s "$FIXTURE/ssh-pid" ]]; then break; fi
 sleep 0.01
done
ssh_pid=$(cat "$FIXTURE/ssh-pid")
if wait_cleanup_process "$pid" 1; then exit 1; fi

# A shell wrapper is insufficient: the actual SSH process must be the owned
# background PID, and it must be gone when the bounded wait returns.
if [[ "$pid" != "$ssh_pid" ]]; then
 echo "async PID $pid differs from SSH PID $ssh_pid" >&2
 exit 1
fi
if kill -0 "$ssh_pid" 2>/dev/null; then
 echo "SSH PID $ssh_pid survived the deadline" >&2
 exit 1
fi`)
			if err != nil || !strings.Contains(out, "exceeded local 1s budget") || time.Since(start) > 5*time.Second {
				t.Fatalf("background SSH timeout failed: %v %s (elapsed %s)", err, out, time.Since(start))
			}
			calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
			if strings.TrimSpace(string(calls)) != "SSH" {
				t.Fatalf("unexpected API/effect calls: %s", calls)
			}
		})
	}
}

func TestForegroundSSHReturnsToItsCaller(t *testing.T) {
	f := newSoakFixture(t)
	out, err := f.run(t, `if ssh_node source ignored; then exit 1; else test "$?" = 91; fi
printf 'caller continued\n'`)
	if err != nil || out != "caller continued\n" {
		t.Fatalf("foreground SSH replaced its caller or lost the status: %v %q", err, out)
	}
}

func TestControllerFaultRequiresExactDeploymentOwnerAndNewUID(t *testing.T) {
	for _, name := range []string{"exact restart", "foreign ReplicaSet", "wrong image", "multiple replicas"} {
		t.Run(name, func(t *testing.T) {
			f := newSoakFixture(t)
			controller := true
			pod := f.pod.DeepCopy()
			pod.Name, pod.UID = "controller-pod", "old-controller-uid"
			pod.Spec.Containers[0].Name = "shiftpv-controller"
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "controller-rs", UID: "controller-rs-uid", Controller: &controller}}
			replicas := int32(1)
			if name == "wrong image" {
				pod.Spec.Containers[0].Image = "another-image"
			}
			if name == "multiple replicas" {
				replicas = 2
			}
			rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "controller-rs", Namespace: "system", UID: "controller-rs-uid",
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "shiftpv-controller", UID: "controller-deployment-uid", Controller: &controller}}}}
			if name == "foreign ReplicaSet" {
				rs.OwnerReferences[0].UID = "another-deployment"
			}
			deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "shiftpv-controller", Namespace: "system", UID: "controller-deployment-uid"},
				Spec: appsv1.DeploymentSpec{Replicas: &replicas, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "shiftpv-controller", Image: testNodeImage}}}}}}
			writeJSON(t, filepath.Join(f.dir, "controller-pod.json"), pod)
			writeJSON(t, filepath.Join(f.dir, "controller-rs.json"), rs)
			writeJSON(t, filepath.Join(f.dir, "controller-deployment.json"), deployment)
			out, err := f.run(t, `restart_soak_controller`)
			if (err == nil) != (name == "exact restart") {
				t.Fatalf("controller fault result: %v\n%s", err, out)
			}
			if name != "exact restart" {
				f.noEffects(t)
			} else {
				calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
				if strings.Count(string(calls), "delete --raw ") != 1 {
					t.Fatalf("controller Pod deleted more than once: %s", calls)
				}
			}
		})
	}
}
