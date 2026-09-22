package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestDiscoveryReadsEachOwnerOncePerPass(t *testing.T) {
	for _, n := range []int{10, 100, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			repo := &memoryRepository{volumes: map[string]volumeapi.State{}}
			for i := 0; i < n; i++ {
				repo.volumes[fmt.Sprintf("shiftpv-%032x", i+1)] = volumeapi.State{Phase: volumeapi.PhaseReady, OwnerNode: "source"}
			}
			client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source"}})
			r := newTestReconciler(client, repo)
			if err := r.ReconcileAll(context.Background()); err != nil {
				t.Fatal(err)
			}
			gets := 0
			for _, a := range client.Actions() {
				if a.GetVerb() == "get" && a.GetResource().Resource == "nodes" {
					gets++
				}
			}
			t.Logf("Ready volumes=%d, distinct owners=1, Node GETs per full reconcile=%d", n, gets)
			if gets != 1 {
				t.Fatalf("unexpected Node GET count: %d", gets)
			}
		})
	}
}

func TestPoolObservationUsesOneListPerSnapshot(t *testing.T) {
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "source-pool", "uid": "source-pool-uid"},
		"spec":     map[string]any{"nodeName": "source", "mountPath": "/pool"},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.PoolResource: "ShiftPVPoolList"}, pool)
	r := newTestReconciler(fake.NewSimpleClientset(), &volumeapi.Registry{Client: dynamicClient})
	for i := 0; i < 10; i++ {
		if _, err := r.observePools(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	lists := 0
	for _, a := range dynamicClient.Actions() {
		if a.GetVerb() == "list" && a.GetResource() == volumeapi.PoolResource {
			lists++
		}
	}
	t.Logf("Pool snapshots=10, Pool LIST requests=%d", lists)
	if lists != 10 {
		t.Fatalf("unexpected LIST count: %d", lists)
	}
}

func TestDiscoveryDoesNotCacheOwnerAcrossPasses(t *testing.T) {
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	fixture := newMobilityFixture(id)
	fixture.SourceNode.Spec.Unschedulable = false
	client := fake.NewSimpleClientset(fixture.Objects()...)
	repo := &memoryRepository{
		volumes: map[string]volumeapi.State{id: {Phase: volumeapi.PhaseReady, OwnerNode: "source", PublishedNodes: []string{"source"}}},
		pools:   []volumeapi.Pool{{NodeName: "source", MountPath: "/pool"}, {NodeName: "destination", MountPath: "/pool"}},
	}
	r := newTestReconciler(client, repo)
	if err := r.discoverMoves(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.moves) != 0 {
		t.Fatal("uncordoned owner created a Move")
	}
	fixture.SourceNode.Spec.Unschedulable = true
	if _, err := client.CoreV1().Nodes().Update(context.Background(), fixture.SourceNode, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.discoverMoves(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.moves) != 1 {
		t.Fatal("next pass reused the old owner observation")
	}
}

func TestNamedPVObservationKeepsBindingChecksWithoutListing(t *testing.T) {
	for _, scenario := range []string{"valid", "missing", "wrong handle", "replaced claim", "deleting"} {
		t.Run(scenario, func(t *testing.T) {
			id := "shiftpv-0123456789abcdef0123456789abcdef"
			f := newMobilityFixture(id)
			switch scenario {
			case "missing":
				f.PV = nil
			case "wrong handle":
				f.PV.Spec.CSI.VolumeHandle = "another-volume"
			case "replaced claim":
				f.Claim.UID = "replacement"
			case "deleting":
				now := metav1.Now()
				f.PV.DeletionTimestamp = &now
			}
			client := fake.NewSimpleClientset(f.Objects()...)
			r := newTestReconciler(client, &memoryRepository{})
			move := volumeapi.Move{Spec: volumeapi.MoveSpec{VolumeID: id, SourceNode: "source"}, Status: volumeapi.MoveStatus{PersistentVolumeName: "pv"}}
			observed := observation{Volume: volumeapi.State{OwnerNode: "source"}}
			done, err := r.observeBinding(context.Background(), move, &observed)
			if err != nil {
				t.Fatal(err)
			}
			if done != (scenario != "valid") {
				t.Fatalf("done=%v for %s", done, scenario)
			}
			for _, a := range client.Actions() {
				if a.GetResource().Resource == "persistentvolumes" && a.GetVerb() == "list" {
					t.Fatal("named PV triggered a full LIST")
				}
			}
		})
	}
}
