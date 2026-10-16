package volumeapi

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func preparedMoveFixture() Move {
	return Move{
		Name: "move", UID: "move-uid", ResourceVersion: "7", Finalizers: []string{MoveProtectionFinalizer},
		Spec: MoveSpec{ProtocolVersion: MoveProtocolPreparedV1, VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", SourceNode: "source"},
		Status: MoveStatus{
			Phase: "Preparing", PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid",
			ClaimNamespace: "workload", ClaimName: "claim", ClaimUID: "claim-uid", ConsumerName: "consumer", ConsumerUID: "consumer-uid",
			PlacementPodUID: "placement-uid", CandidateNodes: []string{"destination"},
			Preparation: &MovePreparation{Pool: PoolReference{Name: "destination-pool", UID: "destination-pool-uid"}, NodeName: "destination", ReservedBytes: 1024},
			SourceCopy: &volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", NodeName: "source",
				VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "source-copy", Role: volume.RoleServing},
		},
	}
}

func preparedMoveObject(move Move) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": move.Name, "uid": move.UID, "resourceVersion": move.ResourceVersion},
		"spec":     map[string]any{"volumeID": move.Spec.VolumeID, "sourceNode": move.Spec.SourceNode, "recovery": move.Spec.Recovery},
	}}
	object.SetFinalizers(move.Finalizers)
	if move.Spec.ProtocolVersion != "" {
		object.Object["spec"].(map[string]any)["protocolVersion"] = move.Spec.ProtocolVersion
	}
	setMoveStatus(object, move.Status)
	return object
}

func TestPreparedCapacityRequiresProtectionAndCancellationNoJournal(t *testing.T) {
	for _, boundary := range []string{"preparation without finalizer", "approval without finalizer", "cancel with empty journal"} {
		t.Run(boundary, func(t *testing.T) {
			move := preparedMoveFixture()
			move.Finalizers = nil
			next := move.Status
			switch boundary {
			case "preparation without finalizer":
				move.Status.Preparation = nil
			case "approval without finalizer":
				move.Status.Phase = "WaitingForCapacity"
				next = approvedPreparation(move)
			case "cancel with empty journal":
				move.Status.Phase = "Canceling"
				next.Phase, next.Preparation = "Cancelled", nil
			}
			object := preparedMoveObject(move)
			if boundary == "cancel with empty journal" {
				object.Object["status"].(map[string]any)["cleanup"] = map[string]any{}
			}
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			registry := &Registry{Client: client}
			if _, err := registry.CompareAndSetMoveStatus(context.Background(), move, next); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("unprotected authority change accepted: %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "update" {
					t.Fatal("unsafe status was written")
				}
			}
		})
	}
}

func TestPreparedMoveCodecAndLegacyShape(t *testing.T) {
	move := preparedMoveFixture()
	decoded, err := moveFrom(preparedMoveObject(move))
	if err != nil || !reflect.DeepEqual(decoded, move) {
		t.Fatalf("roundtrip = %#v, %v", decoded, err)
	}
	for _, value := range []any{nil, "not-an-object", map[string]any{}, map[string]any{"pool": map[string]any{"name": "pool", "uid": "uid"}, "nodeName": "node", "reservedBytes": "1024"}} {
		object := preparedMoveObject(move)
		object.Object["status"].(map[string]any)["preparation"] = value
		decoded, err := moveFrom(object)
		if value == nil {
			if err != nil || decoded.Status.Preparation != nil {
				t.Fatalf("null preparation = %#v, %v", decoded, err)
			}
		} else if err == nil {
			t.Fatalf("malformed preparation accepted: %#v", value)
		}
	}
	object := preparedMoveObject(Move{Spec: MoveSpec{VolumeID: "legacy", SourceNode: "source"}})
	for _, key := range []string{"preparation", "persistentVolumeUID", "persistentVolumeClaimUID", "placementPodUID"} {
		if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "status", key); found {
			t.Fatalf("legacy status includes new field %q", key)
		}
	}
	if _, found, _ := unstructured.NestedString(object.Object, "spec", "protocolVersion"); found {
		t.Fatal("legacy spec gained protocolVersion")
	}
	object.Object["spec"].(map[string]any)["protocolVersion"] = "future-v2"
	decoded, err = moveFrom(object)
	if err != nil || decoded.Spec.ProtocolVersion != "future-v2" {
		t.Fatalf("unknown protocol must remain visible to isolation guard: %#v %v", decoded, err)
	}
}

func TestValidateMovePreparationRejectsContradictoryAuthority(t *testing.T) {
	cases := map[string]func(*Move){
		"protocol":             func(m *Move) { m.Spec.ProtocolVersion = "" },
		"unknown protocol":     func(m *Move) { m.Spec.ProtocolVersion = "future-v2" },
		"move UID":             func(m *Move) { m.UID = "" },
		"pool UID":             func(m *Move) { m.Status.Preparation.Pool.UID = "" },
		"pool name":            func(m *Move) { m.Status.Preparation.Pool.Name = "" },
		"node":                 func(m *Move) { m.Status.Preparation.NodeName = "" },
		"bytes":                func(m *Move) { m.Status.Preparation.ReservedBytes = 0 },
		"PV UID":               func(m *Move) { m.Status.PersistentVolumeUID = "" },
		"claim UID":            func(m *Move) { m.Status.ClaimUID = "" },
		"consumer UID":         func(m *Move) { m.Status.ConsumerUID = "" },
		"placement UID":        func(m *Move) { m.Status.PlacementPodUID = "" },
		"source missing":       func(m *Move) { m.Status.SourceCopy = nil },
		"source volume":        func(m *Move) { m.Spec.VolumeID = "shiftpv-ffffffffffffffffffffffffffffffff" },
		"source node":          func(m *Move) { m.Spec.SourceNode = "other-node" },
		"source role":          func(m *Move) { m.Status.SourceCopy.Role = volume.RoleIncoming },
		"source pool":          func(m *Move) { m.Status.Preparation.Pool.UID = m.Status.SourceCopy.PoolUID },
		"destination mismatch": func(m *Move) { m.Status.DestinationNode = "other-node" },
		"pool mismatch":        func(m *Move) { m.Status.DestinationPoolUID = "other-pool" },
		"approval":             func(m *Move) { m.Status.CapacityApproved = true },
		"incoming":             func(m *Move) { m.Status.IncomingCopy = m.Status.SourceCopy },
		"destination":          func(m *Move) { m.Status.DestinationCopy = m.Status.SourceCopy },
		"copy operation":       func(m *Move) { m.Status.CopyOperationID = "copy" },
		"promotion operation":  func(m *Move) { m.Status.PromotionOperationID = "promote" },
		"copy job":             func(m *Move) { m.Status.CopyJobName = "copy" },
		"promotion job":        func(m *Move) { m.Status.PromotionJobName = "promote" },
		"cleanup":              func(m *Move) { m.Status.CleanupPhase = "Running" },
		"rollback":             func(m *Move) { m.Status.RollbackRequiredGeneration = 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			move := preparedMoveFixture()
			mutate(&move)
			if err := ValidateMovePreparation(move); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("invalid reservation accepted: %v", err)
			}
		})
	}
	move := preparedMoveFixture()
	move.Status.Preparation.NodeName = move.Spec.SourceNode
	if err := ValidateMovePreparation(move); err != nil {
		t.Fatalf("independent Pool on the same node was rejected: %v", err)
	}
}

func TestMovePreparationTransitionBoundaries(t *testing.T) {
	for _, field := range []string{"pool", "node", "bytes", "PV UID", "claim UID", "consumer UID", "placement UID", "clear"} {
		t.Run(field, func(t *testing.T) {
			current, desired := preparedMoveFixture(), preparedMoveFixture()
			switch field {
			case "pool":
				desired.Status.Preparation.Pool = PoolReference{Name: "other", UID: "other-uid"}
			case "node":
				desired.Status.Preparation.NodeName = "other"
			case "bytes":
				desired.Status.Preparation.ReservedBytes++
			case "PV UID":
				desired.Status.PersistentVolumeUID = "other"
			case "claim UID":
				desired.Status.ClaimUID = "other"
			case "consumer UID":
				desired.Status.ConsumerUID = "other"
			case "placement UID":
				desired.Status.PlacementPodUID = "other"
			case "clear":
				desired.Status.Preparation = nil
			}
			if err := validatePreparedTransition(current, desired.Status); err == nil {
				t.Fatal("changed prepared authority accepted")
			}
		})
	}
	current := preparedMoveFixture()
	next := current.Status
	next.Phase = "Canceling"
	if err := validatePreparedTransition(current, next); err != nil {
		t.Fatal(err)
	}
	current.Status = next
	next.Phase, next.Preparation = "Cancelled", nil
	if err := validatePreparedTransition(current, next); err != nil {
		t.Fatal(err)
	}
	current = preparedMoveFixture()
	for _, phase := range []string{"Preparing", "StartingDisruption", "Canceling", "Cancelled", "unknown", "WaitingForCapacity"} {
		current.Status.Phase = phase
		next = approvedPreparation(current)
		err := validatePreparedTransition(current, next)
		if (phase == "WaitingForCapacity") != (err == nil) {
			t.Fatalf("phase %s conversion: %v", phase, err)
		}
	}
	current.Status.Phase = "WaitingForCapacity"
	next = approvedPreparation(current)
	next.DestinationCopy.PoolName = "wrong-name"
	if err := validatePreparedTransition(current, next); err == nil {
		t.Fatal("different Pool name was admitted")
	}
	current.Status.Preparation = nil
	next = approvedPreparation(preparedMoveFixture())
	if err := validatePreparedTransition(current, next); err == nil {
		t.Fatal("execution approval bypassed preparation")
	}
}

func approvedPreparation(move Move) MoveStatus {
	next := move.Status
	prep := next.Preparation
	next.Preparation, next.CapacityApproved = nil, true
	next.Phase, next.DestinationNode, next.DestinationPoolUID = "Copying", prep.NodeName, prep.Pool.UID
	incoming, serving := MoveTransactionCopies(move.UID, move.Spec.VolumeID, MoveDestinationAnchor{
		InstallationID: next.SourceCopy.InstallationID, PoolName: prep.Pool.Name, PoolUID: prep.Pool.UID,
		NodeName: prep.NodeName, VolumeUID: next.SourceCopy.VolumeUID,
	})
	next.IncomingCopy, next.DestinationCopy = &incoming, &serving
	next.CopyOperationID, next.PromotionOperationID = MoveCopyOperationID(move.UID), MovePromotionOperationID(move.UID)
	return next
}

func TestCompareAndSetMoveStatusReturnsStoredVersionWithoutReplaying(t *testing.T) {
	for _, outcome := range []string{"success", "conflict", "lost response"} {
		t.Run(outcome, func(t *testing.T) {
			move := preparedMoveFixture()
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), preparedMoveObject(move))
			registry := &Registry{Client: client}
			writes := 0
			client.PrependReactor("update", "shiftpvmoves", func(action k8stesting.Action) (bool, runtime.Object, error) {
				writes++
				update := action.(k8stesting.UpdateActionImpl)
				if update.GetSubresource() != "status" || update.GetUpdateOptions().FieldValidation != metav1.FieldValidationStrict {
					t.Fatalf("unsafe update options: %#v", update)
				}
				stored := update.GetObject().(*unstructured.Unstructured).DeepCopy()
				if stored.GetResourceVersion() != "7" {
					t.Fatal("wrong expected resourceVersion")
				}
				if outcome == "conflict" {
					concurrent := preparedMoveObject(move)
					concurrent.SetResourceVersion("8")
					concurrent.Object["status"].(map[string]any)["cleanup"] = map[string]any{"status": map[string]any{"phase": "Running"}}
					if err := client.Tracker().Update(MoveResource, concurrent, ""); err != nil {
						t.Fatal(err)
					}
					return true, nil, apierrors.NewConflict(MoveResource.GroupResource(), move.Name, errors.New("cleanup writer advanced resourceVersion"))
				}
				stored.SetResourceVersion("8")
				if err := client.Tracker().Update(MoveResource, stored, ""); err != nil {
					t.Fatal(err)
				}
				if outcome == "lost response" {
					return true, nil, errors.New("response lost after storage")
				}
				return true, stored, nil
			})
			next := move.Status
			next.Phase = "StartingDisruption"
			stored, err := registry.CompareAndSetMoveStatus(context.Background(), move, next)
			if writes != 1 {
				t.Fatalf("writes=%d", writes)
			}
			if outcome == "success" {
				if err != nil || stored.ResourceVersion != "8" || stored.Status.Phase != next.Phase {
					t.Fatalf("stored=%#v err=%v", stored, err)
				}
			} else if err == nil {
				t.Fatal("ambiguous/conflicting update was adopted")
			}
			observed, getErr := registry.GetMove(context.Background(), move.Name)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if outcome == "conflict" && (observed.Status.CleanupPhase != "Running" || observed.Status.Phase != move.Status.Phase) {
				t.Fatal("concurrent cleanup journal was overwritten")
			}
			if outcome != "conflict" && observed.Status.Phase != next.Phase {
				t.Fatal("accepted write not persisted")
			}
			if outcome == "success" {
				if _, err := registry.CompareAndSetMoveStatus(context.Background(), move, next); !errors.Is(err, ErrStateConflict) {
					t.Fatalf("stale identical next adopted: %v", err)
				}
				if writes != 1 {
					t.Fatal("stale call wrote again")
				}
			}
		})
	}
}

func TestCompareAndSetMoveStatusRejectsChangedIdentityAndSpec(t *testing.T) {
	for _, field := range []string{"UID", "resourceVersion", "source", "recovery", "protocol", "missing UID", "missing resourceVersion"} {
		t.Run(field, func(t *testing.T) {
			expected := preparedMoveFixture()
			object := preparedMoveObject(expected)
			switch field {
			case "UID":
				object.SetUID("replacement")
			case "resourceVersion":
				object.SetResourceVersion("8")
			case "source":
				object.Object["spec"].(map[string]any)["sourceNode"] = "other"
			case "recovery":
				object.Object["spec"].(map[string]any)["recovery"] = "ResumeOwner"
			case "protocol":
				object.Object["spec"].(map[string]any)["protocolVersion"] = "future-v2"
			case "missing UID":
				expected.UID = ""
			case "missing resourceVersion":
				expected.ResourceVersion = ""
			}
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			registry := &Registry{Client: client}
			if _, err := registry.CompareAndSetMoveStatus(context.Background(), expected, expected.Status); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("changed %s accepted: %v", field, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "update" {
					t.Fatal("stale authority wrote status")
				}
			}
		})
	}
}

func TestCompareAndSetMoveStatusPreservesRawCleanup(t *testing.T) {
	move := preparedMoveFixture()
	move.Status.Preparation = nil
	object := preparedMoveObject(move)
	cleanup := map[string]any{"spec": map[string]any{"unknownFutureIdentity": "retained"}, "status": map[string]any{"phase": "Completed", "receipt": map[string]any{"digest": "evidence"}}}
	object.Object["status"].(map[string]any)["cleanup"] = cleanup
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	registry := &Registry{Client: client}
	next := move.Status
	next.Reason, next.CleanupPhase = "unchanged journal", "Running"
	if _, err := registry.CompareAndSetMoveStatus(context.Background(), move, next); err != nil {
		t.Fatal(err)
	}
	stored, err := client.Resource(MoveResource).Get(context.Background(), move.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored.Object["status"].(map[string]any)["cleanup"], cleanup) {
		t.Fatal("raw cleanup journal changed")
	}
}

func TestCreatePreparedMoveUsesStrictSchemaAndDetectsPruning(t *testing.T) {
	for _, prune := range []bool{false, true} {
		client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		client.PrependReactor("create", "shiftpvmoves", func(action k8stesting.Action) (bool, runtime.Object, error) {
			create := action.(k8stesting.CreateActionImpl)
			if create.GetCreateOptions().FieldValidation != metav1.FieldValidationStrict {
				t.Fatal("prepared Create is not strict")
			}
			object := create.GetObject().(*unstructured.Unstructured).DeepCopy()
			object.SetName("move-created")
			if prune {
				delete(object.Object["spec"].(map[string]any), "protocolVersion")
			}
			return true, object, nil
		})
		registry := &Registry{Client: client}
		move, err := registry.CreateMove(context.Background(), "move-", preparedMoveFixture().Spec)
		if prune {
			if err == nil || !strings.Contains(err.Error(), "not stored") {
				t.Fatalf("pruned protocol accepted: %v", err)
			}
		} else if err != nil || move.Spec.ProtocolVersion != MoveProtocolPreparedV1 {
			t.Fatalf("created=%#v err=%v", move, err)
		}
	}
}

func TestMoveCleanupSettledRetainsPreparedAndUnknownRecords(t *testing.T) {
	for _, phase := range []string{"Succeeded", "Blocked"} {
		move := preparedMoveFixture()
		move.Status.Phase, move.Status.CleanupPhase = phase, "Completed"
		move.Status.RecoveryPhase, move.Status.CapacityReason = "Recovered", "RecoverySettled"
		if MoveCleanupSettled(move) {
			t.Fatal("logical hold released by terminal label")
		}
		move.Status.Preparation = nil
		if !MoveCleanupSettled(move) {
			t.Fatal("settled known protocol not recognized")
		}
		move.Spec.ProtocolVersion = "future-v2"
		if MoveCleanupSettled(move) {
			t.Fatal("unknown protocol considered settled")
		}
	}
}

func TestCompareAndSetMoveStatusRejectsPrunedResponseWithoutReplay(t *testing.T) {
	for _, field := range []string{"preparation", "persistentVolumeUID", "persistentVolumeClaimUID", "placementPodUID", "cleanup"} {
		t.Run(field, func(t *testing.T) {
			move := preparedMoveFixture()
			object := preparedMoveObject(move)
			if field == "cleanup" {
				move.Status.Preparation = nil
				object = preparedMoveObject(move)
				object.Object["status"].(map[string]any)["cleanup"] = map[string]any{"status": map[string]any{"phase": "Completed", "receipt": map[string]any{"digest": "kept"}}}
			}
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			writes := 0
			client.PrependReactor("update", "shiftpvmoves", func(action k8stesting.Action) (bool, runtime.Object, error) {
				writes++
				stored := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				delete(stored.Object["status"].(map[string]any), field)
				stored.SetResourceVersion("8")
				return true, stored, nil
			})
			registry := &Registry{Client: client}
			if _, err := registry.CompareAndSetMoveStatus(context.Background(), move, move.Status); err == nil {
				t.Fatalf("pruned %s response accepted", field)
			}
			if writes != 1 {
				t.Fatalf("uncertain write replayed: %d", writes)
			}
		})
	}
}

func TestDeletingMoveCannotAcquireNewAuthority(t *testing.T) {
	for _, operation := range []string{"grant preparation", "approve execution", "retain hold", "cancel hold"} {
		t.Run(operation, func(t *testing.T) {
			move := preparedMoveFixture()
			next := move.Status
			switch operation {
			case "grant preparation":
				move.Status.Preparation = nil
			case "approve execution":
				move.Status.Phase = "WaitingForCapacity"
				next = approvedPreparation(move)
			case "cancel hold":
				move.Status.Phase = "Canceling"
				next.Phase, next.Preparation = "Cancelled", nil
			}
			object := preparedMoveObject(move)
			now := metav1.Now()
			object.SetDeletionTimestamp(&now)
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			registry := &Registry{Client: client}
			_, err := registry.CompareAndSetMoveStatus(context.Background(), move, next)
			acquires := operation == "grant preparation" || operation == "approve execution"
			if acquires != (err != nil) {
				t.Fatalf("operation %s: %v", operation, err)
			}
			if acquires {
				for _, action := range client.Actions() {
					if action.GetVerb() == "update" {
						t.Fatal("deleting Move acquired authority")
					}
				}
			}
		})
	}
}

func TestPreparedExecutionHoldRequiresDurableSettlement(t *testing.T) {
	for _, scenario := range []string{"completed cleanup", "recorded absence", "ordinary phase", "wrong recovery", "advance phase", "missing reason", "running", "needs review", "missing scan", "new scan only"} {
		t.Run(scenario, func(t *testing.T) {
			move := preparedMoveFixture()
			move.Status = approvedPreparation(move)
			move.Status.Phase, move.Status.RecoveryPhase = "Blocked", "Retiring"
			move.Status.CleanupPhase = "Completed"
			if scenario == "ordinary phase" {
				move.Status.Phase = "Copying"
			}
			if scenario == "wrong recovery" {
				move.Status.RecoveryPhase = "Resuming"
			}
			if scenario == "running" {
				move.Status.CleanupPhase = "Running"
			}
			if scenario == "needs review" {
				move.Status.CleanupPhase = "NeedsReview"
			}
			if scenario == "recorded absence" || scenario == "missing scan" || scenario == "new scan only" {
				move.Status.CleanupPhase = ""
			}
			if scenario == "recorded absence" {
				move.Status.RollbackRequiredGeneration = 3
			}
			next := move.Status
			next.CapacityApproved, next.CapacityReason = false, "RecoverySettled"
			if scenario == "missing reason" {
				next.CapacityReason = ""
			}
			if scenario == "advance phase" {
				next.RecoveryPhase = "Recovered"
			}
			if scenario == "new scan only" {
				next.RollbackRequiredGeneration = 3
			}
			err := validatePreparedTransition(move, next)
			valid := scenario == "completed cleanup" || scenario == "recorded absence"
			if valid != (err == nil) {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}
