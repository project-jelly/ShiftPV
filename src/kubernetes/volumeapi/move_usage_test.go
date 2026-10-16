package volumeapi

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func sourceUsageFixture(t *testing.T, protocol, phase string) (*Registry, *dynamicfake.FakeDynamicClient, Move, volume.CopyIdentity) {
	t.Helper()
	move := preparedMoveFixture()
	copy := *move.Status.SourceCopy
	move.Spec.ProtocolVersion, move.Status.Phase = protocol, phase
	if protocol == "" {
		move.Status.Preparation, move.Status.SourceCopy = nil, nil
	}
	object := preparedMoveObject(move)
	object.Object["status"].(map[string]any)["futureObservation"] = map[string]any{"keep": "exact"}
	state := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": move.Spec.VolumeID, "uid": copy.VolumeUID, "resourceVersion": "4", "finalizers": []any{VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": move.Spec.VolumeID},
	}}
	setState(state, State{Phase: PhaseMoving, OwnerNode: move.Spec.SourceNode, ActiveMove: move.Name, CurrentCopy: &copy})
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object, state)
	client.PrependReactor("update", "shiftpvmoves", func(action ktesting.Action) (bool, runtime.Object, error) {
		stored, err := storeUsageUpdate(client, action)
		return true, stored, err
	})
	return &Registry{Client: client}, client, move, copy
}

func storeUsageUpdate(client *dynamicfake.FakeDynamicClient, action ktesting.Action) (*unstructured.Unstructured, error) {
	update := action.(ktesting.UpdateAction)
	if update.GetSubresource() != "status" {
		return nil, fmt.Errorf("source usage must use the status subresource")
	}
	wanted := update.GetObject().(*unstructured.Unstructured)
	stored, err := client.Tracker().Get(MoveResource, "", wanted.GetName())
	if err != nil {
		return nil, err
	}
	current := stored.(*unstructured.Unstructured)
	if wanted.GetUID() != current.GetUID() || wanted.GetResourceVersion() != current.GetResourceVersion() {
		return nil, apierrors.NewConflict(MoveResource.GroupResource(), wanted.GetName(), errors.New("UID/resourceVersion changed"))
	}
	version, err := strconv.Atoi(current.GetResourceVersion())
	if err != nil {
		return nil, err
	}
	next := wanted.DeepCopy()
	next.SetResourceVersion(strconv.Itoa(version + 1))
	return next, client.Tracker().Update(MoveResource, next, "")
}

func TestRecordSourceUsageChangesOnlyExactMeasurement(t *testing.T) {
	for _, test := range []struct{ protocol, phase string }{{MoveProtocolPreparedV1, "WaitingForCapacity"}} {
		t.Run(test.protocol+"/"+test.phase, func(t *testing.T) {
			ctx := context.Background()
			r, client, move, copy := sourceUsageFixture(t, test.protocol, test.phase)
			before, _ := client.Resource(MoveResource).Get(ctx, move.Name, metav1.GetOptions{})
			got, err := r.RecordSourceUsage(ctx, move, copy, 4096)
			if err != nil || got.ResourceVersion != "8" || got.Status.SourceBytes != 4096 || got.Status.SourceCopy == nil || *got.Status.SourceCopy != copy {
				t.Fatalf("measurement not stored: move=%+v err=%v", got, err)
			}
			after, _ := client.Resource(MoveResource).Get(ctx, move.Name, metav1.GetOptions{})
			want := before.DeepCopy()
			encoded, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
			_ = unstructured.SetNestedMap(want.Object, encoded, "status", "sourceCopy")
			_ = unstructured.SetNestedField(want.Object, int64(4096), "status", "sourceBytes")
			want.SetResourceVersion("8")
			if !reflect.DeepEqual(after.Object, want.Object) {
				t.Fatalf("usage overwrote unrelated status: got=%+v want=%+v", after.Object, want.Object)
			}
			client.ClearActions()
			if _, err := r.RecordSourceUsage(ctx, got, copy, 4096); err != nil {
				t.Fatal(err)
			}
			assertNoUsageWrites(t, client)
		})
	}
}

func TestRecordSourceUsageRejectsChangedMoveAndAuthority(t *testing.T) {
	mutations := map[string]func(*Move, *unstructured.Unstructured, *unstructured.Unstructured, *volume.CopyIdentity, *int64){
		"missing UID": func(m *Move, _, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) { m.UID = "" },
		"missing RV": func(m *Move, _, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			m.ResourceVersion = ""
		},
		"Move replaced": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			m.SetUID("replacement")
		},
		"Move RV changed": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			m.SetResourceVersion("8")
		},
		"spec changed": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(m.Object, "ResumeOwner", "spec", "recovery")
		},
		"Move deleting": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			now := metav1.Now()
			m.SetDeletionTimestamp(&now)
		},
		"Move unprotected": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) { m.SetFinalizers(nil) },
		"phase changed": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(m.Object, "Canceling", "status", "phase")
		},
		"no prepared hold": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			unstructured.RemoveNestedField(m.Object, "status", "preparation")
		},
		"approved": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(m.Object, true, "status", "capacityApproved")
		},
		"disk intent": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(m.Object, "copy-job", "status", "copyJobName")
		},
		"raw empty cleanup": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedMap(m.Object, map[string]any{}, "status", "cleanup")
		},
		"missing recorded copy": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			unstructured.RemoveNestedField(m.Object, "status", "sourceCopy")
		},
		"different measured copy": func(_ *Move, _, _ *unstructured.Unstructured, c *volume.CopyIdentity, _ *int64) {
			c.CopyID = "replacement"
		},
		"non-serving copy": func(_ *Move, _, _ *unstructured.Unstructured, c *volume.CopyIdentity, _ *int64) {
			c.Role = volume.RoleIncoming
		},
		"zero bytes":     func(_ *Move, _, _ *unstructured.Unstructured, _ *volume.CopyIdentity, b *int64) { *b = 0 },
		"negative bytes": func(_ *Move, _, _ *unstructured.Unstructured, _ *volume.CopyIdentity, b *int64) { *b = -1 },
		"stored bytes differ": func(_ *Move, m, _ *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(m.Object, int64(1024), "status", "sourceBytes")
		},
		"Volume replaced": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			v.SetUID("replacement")
		},
		"Volume deleting": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			now := metav1.Now()
			v.SetDeletionTimestamp(&now)
		},
		"Volume cleanup": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedMap(v.Object, map[string]any{}, "status", "cleanup")
		},
		"Volume unprotected": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) { v.SetFinalizers(nil) },
		"Volume ready": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(v.Object, "Ready", "status", "phase")
		},
		"owner changed": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(v.Object, "destination", "status", "ownerNode")
		},
		"ActiveMove changed": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(v.Object, "other", "status", "activeMove")
		},
		"currentCopy changed": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedField(v.Object, "other", "status", "currentCopy", "copyID")
		},
		"source republished": func(_ *Move, _, v *unstructured.Unstructured, _ *volume.CopyIdentity, _ *int64) {
			_ = unstructured.SetNestedStringSlice(v.Object, []string{"source"}, "status", "publishedNodes")
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r, client, move, copy := sourceUsageFixture(t, MoveProtocolPreparedV1, "WaitingForCapacity")
			object, _ := client.Resource(MoveResource).Get(context.Background(), move.Name, metav1.GetOptions{})
			state, _ := client.Resource(VolumeResource).Get(context.Background(), move.Spec.VolumeID, metav1.GetOptions{})
			bytes := int64(4096)
			mutate(&move, object, state, &copy, &bytes)
			if err := client.Tracker().Update(MoveResource, object, ""); err != nil {
				t.Fatal(err)
			}
			if err := client.Tracker().Update(VolumeResource, state, ""); err != nil {
				t.Fatal(err)
			}
			if _, err := r.RecordSourceUsage(context.Background(), move, copy, bytes); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("changed authority was accepted: %v", err)
			}
			assertNoUsageWrites(t, client)
		})
	}
}

func TestRecordSourceUsageRejectsUnsupportedStage(t *testing.T) {
	for _, test := range []struct{ protocol, phase string }{
		{"", "WaitingForDestination"}, {"", "WaitingForCapacity"}, {"future-v2", "WaitingForCapacity"}, {MoveProtocolPreparedV1, "WaitingForDestination"},
		{MoveProtocolPreparedV1, "StartingDisruption"}, {"", "Copying"}, {"", "Blocked"},
	} {
		t.Run(test.protocol+"/"+test.phase, func(t *testing.T) {
			r, client, move, copy := sourceUsageFixture(t, test.protocol, test.phase)
			if _, err := r.RecordSourceUsage(context.Background(), move, copy, 4096); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("unsupported measurement stage accepted: %v", err)
			}
			assertNoUsageWrites(t, client)
		})
	}
}

func TestRecordSourceUsageStopsAfterConflictOrResponseLoss(t *testing.T) {
	for _, outcome := range []string{"conflict with cleanup", "response lost", "response pruned"} {
		t.Run(outcome, func(t *testing.T) {
			r, client, move, copy := sourceUsageFixture(t, MoveProtocolPreparedV1, "WaitingForCapacity")
			writes := 0
			lost := errors.New("usage response lost")
			client.PrependReactor("update", "shiftpvmoves", func(action ktesting.Action) (bool, runtime.Object, error) {
				writes++
				if action.(ktesting.UpdateActionImpl).GetUpdateOptions().FieldValidation != metav1.FieldValidationStrict {
					t.Fatal("usage status update is not strict")
				}
				if outcome == "conflict with cleanup" {
					object, _ := client.Tracker().Get(MoveResource, "", move.Name)
					current := object.(*unstructured.Unstructured).DeepCopy()
					current.SetResourceVersion("8")
					_ = unstructured.SetNestedMap(current.Object, map[string]any{"keep": "concurrent evidence"}, "status", "cleanup")
					if err := client.Tracker().Update(MoveResource, current, ""); err != nil {
						t.Fatal(err)
					}
					return true, nil, apierrors.NewConflict(MoveResource.GroupResource(), move.Name, errors.New("cleanup changed"))
				}
				stored, err := storeUsageUpdate(client, action)
				if err != nil {
					return true, nil, err
				}
				if outcome == "response lost" {
					return true, nil, lost
				}
				response := stored.DeepCopy()
				unstructured.RemoveNestedField(response.Object, "status", "sourceBytes")
				return true, response, nil
			})
			if _, err := r.RecordSourceUsage(context.Background(), move, copy, 4096); err == nil || writes != 1 {
				t.Fatalf("uncertain write was retried/accepted: writes=%d err=%v", writes, err)
			}
			stored, err := r.GetMove(context.Background(), move.Name)
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "conflict with cleanup" {
				object, _ := client.Resource(MoveResource).Get(context.Background(), move.Name, metav1.GetOptions{})
				if stored.Status.SourceBytes != 0 || moveCleanup(object) == nil {
					t.Fatal("concurrent cleanup overwritten")
				}
				return
			}
			if stored.Status.SourceBytes != 4096 || stored.Status.SourceCopy == nil || *stored.Status.SourceCopy != copy {
				t.Fatal("accepted bytes were lost")
			}
			if _, err := r.RecordSourceUsage(context.Background(), move, copy, 4096); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("stale request adopted: %v", err)
			}
			if _, err := r.RecordSourceUsage(context.Background(), stored, copy, 4096); err != nil || writes != 1 {
				t.Fatalf("fresh read-back did not settle without rewrite: writes=%d err=%v", writes, err)
			}
		})
	}
}

func TestMoveStatusWritersCannotEraseOrReplaceSourceUsage(t *testing.T) {
	for _, protocol := range []string{MoveProtocolPreparedV1} {
		for _, bytes := range []int64{0, -1, 8192} {
			t.Run(fmt.Sprintf("%s/%d", protocol, bytes), func(t *testing.T) {
				r, client, move, copy := sourceUsageFixture(t, protocol, "WaitingForCapacity")
				stored, err := r.RecordSourceUsage(context.Background(), move, copy, 4096)
				if err != nil {
					t.Fatal(err)
				}
				client.ClearActions()
				next := stored.Status
				next.SourceBytes = bytes
				_, err = r.CompareAndSetMoveStatus(context.Background(), stored, next)
				if !errors.Is(err, ErrStateConflict) {
					t.Fatalf("measurement rewritten: %v", err)
				}
				assertNoUsageWrites(t, client)
			})
		}
	}
}

func assertNoUsageWrites(t *testing.T, client *dynamicfake.FakeDynamicClient) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "patch" {
			t.Fatalf("unexpected API mutation: %+v", action)
		}
	}
}
