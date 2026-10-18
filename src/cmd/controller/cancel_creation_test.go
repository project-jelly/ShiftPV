package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"reflect"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

const cancellationCommandID = "shiftpv-0123456789abcdef0123456789abcdef"

func cancellationArgs() []string {
	return []string{"--namespace=system", "--volume-id=" + cancellationCommandID, "--volume-uid=volume-uid"}
}

func TestCancelCreationParsesBeforeLoadingClients(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	for _, args := range [][]string{{}, {"--volume-id=bad"}, append(cancellationArgs(), "extra"), append(cancellationArgs(), "--timeout=0"), append(cancellationArgs(), "--node-daemonset-name=bad/name"), append(cancellationArgs(), "--volume-uid=bad/uid"), append(cancellationArgs(), "--namespace=bad/name")} {
		loaded := false
		if err := runCancelCreation(context.Background(), args, io.Discard, func(string, string) (*creation.Canceller, error) { loaded = true; return nil, nil }); err == nil || loaded {
			t.Fatal("invalid identity reached client construction", args, err, loaded)
		}
	}
	if err := runCancelCreation(context.Background(), []string{"--help"}, io.Discard, nil); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
}

func TestCancelCreationUsesExactRegistryDeletionAndReportsGone(t *testing.T) {
	c, api := cancellationCommandFixture(t, "")
	deleted := false
	api.PrependReactor("delete", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
		options := a.(ktesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != "volume-uid" || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "1" {
			t.Fatal("command lost the initialization CAS boundary", options)
		}
		deleted = true
		return false, nil, nil
	})
	var output bytes.Buffer
	err := runCancelCreation(context.Background(), cancellationArgs(), &output, func(ns, ds string) (*creation.Canceller, error) {
		if ns != "system" || ds != "shiftpv-node" {
			t.Fatal(ns, ds)
		}
		return c, nil
	})
	var result struct {
		VolumeID, VolumeUID string
		Complete            bool
	}
	if err != nil || !deleted || json.Unmarshal(output.Bytes(), &result) != nil || !result.Complete || result.VolumeID != cancellationCommandID || result.VolumeUID != "volume-uid" {
		t.Fatal(err, deleted, output.String())
	}
}

func TestCancelCreationPreservesReadyForeignAndUnknownResponsibilities(t *testing.T) {
	for _, scenario := range []string{"Ready", "Moving", "foreign UID", "legacy helper", "inspection unknown", "API unknown"} {
		t.Run(scenario, func(t *testing.T) {
			phase := volumeapi.PhasePending
			if scenario == "Ready" || scenario == "Moving" {
				phase = scenario
			}
			c, api := cancellationCommandFixture(t, phase)
			if scenario == "legacy helper" || scenario == "inspection unknown" {
				c.Inspector = cancellationInspector(func(context.Context, volume.CopyIdentity) (bool, error) {
					if scenario == "inspection unknown" {
						return false, io.ErrUnexpectedEOF
					}
					return true, nil
				})
			}
			if scenario == "API unknown" {
				api.PrependReactor("get", "shiftpvvolumes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, io.ErrUnexpectedEOF })
			}
			args := cancellationArgs()
			if scenario == "foreign UID" {
				args = append(args, "--volume-uid=other-uid")
			}
			before, _ := api.Tracker().Get(volumeapi.VolumeResource, "", cancellationCommandID)
			if err := runCancelCreation(context.Background(), args, io.Discard, func(string, string) (*creation.Canceller, error) { return c, nil }); err == nil {
				t.Fatal("unsupported or unknown state was reported complete")
			}
			after, _ := api.Tracker().Get(volumeapi.VolumeResource, "", cancellationCommandID)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("unsupported responsibility was changed")
			}
			for _, action := range api.Actions() {
				if action.GetVerb() != "get" {
					t.Fatal("unsupported observation granted a mutation", action)
				}
			}
		})
	}
}

func TestCancelCreationTimeoutRetainsDurableIntent(t *testing.T) {
	c, api := cancellationCommandFixture(t, volumeapi.PhasePending)
	err := runCancelCreation(context.Background(), append(cancellationArgs(), "--timeout=30ms"), io.Discard, func(string, string) (*creation.Canceller, error) { return c, nil })
	state, getErr := c.Volumes.Get(context.Background(), cancellationCommandID)
	if !errors.Is(err, context.DeadlineExceeded) || getErr != nil || !state.CancellingCreation() || state.CreationClaim != nil {
		t.Fatal(err, getErr, state)
	}
	for _, action := range api.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("timeout released unfinished cleanup")
		}
	}
}

func TestCancelCreationDoesNotLoadAfterCancellationOrHideClientFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loaded := false
	if err := runCancelCreation(ctx, cancellationArgs(), io.Discard, func(string, string) (*creation.Canceller, error) { loaded = true; return nil, nil }); !errors.Is(err, context.Canceled) || loaded {
		t.Fatal(err, loaded)
	}
	if err := runCancelCreation(context.Background(), cancellationArgs(), io.Discard, func(string, string) (*creation.Canceller, error) { return nil, io.ErrUnexpectedEOF }); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := loadCreationCancellation("system", "node"); err == nil {
		t.Fatal("missing cluster configuration was accepted")
	}
}

type cancellationInspector func(context.Context, volume.CopyIdentity) (bool, error)

func (f cancellationInspector) CreationExists(ctx context.Context, copy volume.CopyIdentity) (bool, error) {
	return f(ctx, copy)
}

type pendingCancellationCleanup struct{}

func (pendingCancellationCleanup) Advance(context.Context, cleanupapi.Spec) (bool, error) {
	return false, nil
}

func cancellationCommandFixture(t *testing.T, phase string) (*creation.Canceller, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", NodeName: "worker", VolumeID: cancellationCommandID, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", Role: volume.RoleServing}
	identity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	if err != nil {
		t.Fatal(err)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": cancellationCommandID, "uid": copy.VolumeUID, "resourceVersion": "1", "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": cancellationCommandID, "requestName": "pvc-original", "initialNode": "worker", "capacityBytes": int64(1024), "initialPool": map[string]any{"name": "pool", "uid": "pool-uid"}},
	}}
	if phase != "" {
		object.Object["status"] = map[string]any{"phase": phase, "ownerNode": "worker", "currentCopy": identity, "creationOperationID": "create-volume-uid"}
	}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &volumeapi.Registry{Client: api}
	return &creation.Canceller{Volumes: registry, Inspector: cancellationInspector(func(context.Context, volume.CopyIdentity) (bool, error) { return false, nil }), Cleanups: pendingCancellationCleanup{}}, api
}
