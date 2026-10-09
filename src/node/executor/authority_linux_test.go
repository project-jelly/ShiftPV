//go:build linux

package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
)

func TestCreationRechecksAuthorityAtEveryBoundary(t *testing.T) {
	for _, change := range []string{"Pool UID", "Pool protection", "Pool path", "Pod UID", "backing"} {
		for boundary := 1; boundary <= 4; boundary++ {
			t.Run(fmt.Sprintf("%s/boundary-%d", change, boundary), func(t *testing.T) {
				client, node, dynamic, kube, state := fixture(t)
				ctx := context.Background()
				node.HostRoot = t.TempDir()
				root := filepath.Join(node.HostRoot, "mnt", "pool")
				if err := os.MkdirAll(root, 0700); err != nil {
					t.Fatal(err)
				}
				if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
					t.Fatal(err)
				}
				poolReads := 0
				backingLost := false
				backingFailure := errors.New("live backing lost")
				node.VerifyPool = func(hostRoot string, pool volumeapi.Pool) error {
					if backingLost {
						return backingFailure
					}
					return readiness.VerifyHostPool(hostRoot, pool)
				}
				dynamic.PrependReactor("get", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
					poolReads++
					// One initial root lookup precedes the four independent checks.
					if poolReads != boundary+1 {
						return false, nil, nil
					}
					if change == "backing" {
						backingLost = true
						return false, nil, nil
					}
					if change == "Pod UID" {
						pod, err := node.Pods.Get(ctx, node.Identity.PodName, metav1.GetOptions{})
						if err != nil {
							return true, nil, err
						}
						pod.UID = types.UID("replacement-pod")
						return false, nil, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace)
					}
					object, err := dynamic.Tracker().Get(volumeapi.PoolResource, "", action.(ktesting.GetAction).GetName())
					if err != nil {
						return true, nil, err
					}
					pool := object.(*unstructured.Unstructured).DeepCopy()
					switch change {
					case "Pool UID":
						pool.SetUID("replacement-pool")
					case "Pool protection":
						pool.SetFinalizers(nil)
					case "Pool path":
						_ = unstructured.SetNestedField(pool.Object, "/mnt/other", "spec", "mountPath")
					}
					return false, nil, dynamic.Tracker().Update(volumeapi.PoolResource, pool, "")
				})
				err := node.execute(ctx, testID)
				want := volumeapi.ErrStateConflict
				if change == "backing" {
					want = backingFailure
				} else if change == "Pool protection" {
					want = volumeapi.ErrPoolNotReady
				}
				if !errors.Is(err, want) || poolReads != boundary+1 {
					t.Fatalf("boundary=%d change=%s reads=%d err=%v", boundary, change, poolReads, err)
				}
				current, err := client.Volumes.Get(ctx, testID)
				if err != nil || current.CreationReceipt != nil || current.Phase != volumeapi.PhaseNodeCreating {
					t.Fatalf("revoked authority recorded completion: %+v, %v", current, err)
				}
				_, statErr := os.Stat(filepath.Join(root, "volumes", testID))
				if boundary <= 2 && !os.IsNotExist(statErr) || boundary >= 3 && statErr != nil {
					t.Fatalf("boundary=%d unexpectedly created or removed serving data: %v", boundary, statErr)
				}
			})
		}
	}
}
