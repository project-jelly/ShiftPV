package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func changeMeasurementObject(t *testing.T, client *dynamicfake.FakeDynamicClient, resource schema.GroupVersionResource, name string, change func(*unstructured.Unstructured)) {
	t.Helper()
	object, err := client.Resource(resource).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	change(object)
	if _, err := client.Resource(resource).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func refreshUsageObservation(object *unstructured.Unstructured, now time.Time) {
	_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
	_ = unstructured.SetNestedField(object.Object, now.UTC().Format(time.RFC3339), "status", "lastProbeTime")
	_ = unstructured.SetNestedField(object.Object, now.UTC().Format(time.RFC3339), "status", "inventory", "observedAt")
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	for _, raw := range conditions {
		raw.(map[string]any)["observedGeneration"] = object.GetGeneration()
	}
	_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
}

func TestUsageReusesMeasurementWhileCurrentObservationCatchesUp(t *testing.T) {
	for _, scenario := range []string{"repeated generations", "expired timestamps", "immediately refreshed generation"} {
		t.Run(scenario, func(t *testing.T) {
			registry, client, options := measurementRegistryFixture(t)
			now := registry.Now()
			registry.Now = func() time.Time { return now }
			options.now = registry.Now
			reads, waits, mounts := 0, 0, 0
			change := func(fn func(*unstructured.Unstructured)) {
				changeMeasurementObject(t, client, volumeapi.PoolResource, options.pool.Name, fn)
			}
			options.wait = func(context.Context) error {
				waits++
				change(func(pool *unstructured.Unstructured) {
					if scenario == "repeated generations" && waits == 1 {
						pool.SetGeneration(3)
						return
					}
					refreshUsageObservation(pool, now)
				})
				return nil
			}
			output, err := measureWithAuthority(context.Background(), registry, options,
				func(_ string, pool volumeapi.Pool) error {
					mounts++
					if pool.Generation != pool.Status.ObservedGeneration {
						t.Fatal("live mount verification used invented generation freshness")
					}
					return nil
				}, func(string, volume.CopyIdentity) error { return nil },
				func(context.Context) (string, error) {
					reads++
					if scenario == "expired timestamps" {
						now = now.Add(2 * time.Minute)
					}
					change(func(pool *unstructured.Unstructured) {
						if scenario != "expired timestamps" {
							pool.SetGeneration(2)
						}
						if scenario == "immediately refreshed generation" {
							refreshUsageObservation(pool, now)
						}
					})
					return "123", nil
				})
			wantWaits := map[string]int{"repeated generations": 2, "expired timestamps": 1, "immediately refreshed generation": 0}[scenario]
			if err != nil || output != "123" || reads != 1 || waits != wantWaits || mounts != 2 {
				t.Fatalf("output=%q err=%v reads=%d waits=%d mounts=%d", output, err, reads, waits, mounts)
			}
		})
	}
}

func TestUsageDiscardsContradictionsObservedWhileWaiting(t *testing.T) {
	for _, scenario := range []string{"own backing false", "own backing unknown", "own backing missing", "invalid inventory", "truncated inventory", "inventory message", "missing inventory", "missing timestamp", "future timestamp", "future generation", "protection removed", "group changed", "limit changed", "backing changed", "copy changed", "published", "marker changed", "live mount changed"} {
		t.Run(scenario, func(t *testing.T) {
			registry, client, options := measurementRegistryFixture(t)
			reads, waits := 0, 0
			options.wait = func(context.Context) error {
				waits++
				resource, name := volumeapi.PoolResource, options.pool.Name
				if scenario == "copy changed" || scenario == "published" {
					resource, name = volumeapi.VolumeResource, options.volumeID
				}
				changeMeasurementObject(t, client, resource, name, func(object *unstructured.Unstructured) {
					switch scenario {
					case "own backing false", "own backing unknown", "own backing missing":
						conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
						var next []any
						for _, raw := range conditions {
							condition := raw.(map[string]any)
							if condition["type"] == volumeapi.PoolConditionCapacityBackingVerified {
								if scenario == "own backing missing" {
									continue
								}
								condition["status"] = "False"
								if scenario == "own backing unknown" {
									condition["status"] = "Unknown"
								}
							}
							next = append(next, condition)
						}
						_ = unstructured.SetNestedSlice(object.Object, next, "status", "conditions")
					case "invalid inventory":
						_ = unstructured.SetNestedField(object.Object, false, "status", "inventory", "valid")
					case "truncated inventory":
						_ = unstructured.SetNestedField(object.Object, true, "status", "inventory", "truncated")
					case "inventory message":
						_ = unstructured.SetNestedField(object.Object, "failed", "status", "inventory", "message")
					case "missing inventory":
						unstructured.RemoveNestedField(object.Object, "status", "inventory")
					case "missing timestamp":
						unstructured.RemoveNestedField(object.Object, "status", "lastProbeTime")
					case "future timestamp":
						_ = unstructured.SetNestedField(object.Object, registry.Now().Add(time.Hour).Format(time.RFC3339), "status", "lastProbeTime")
					case "future generation":
						_ = unstructured.SetNestedField(object.Object, int64(3), "status", "observedGeneration")
					case "protection removed":
						object.SetFinalizers(nil)
					case "group changed":
						_ = unstructured.SetNestedField(object.Object, "other", "spec", "poolGroup")
					case "limit changed":
						_ = unstructured.SetNestedField(object.Object, "2Gi", "spec", "capacity", "limit")
					case "backing changed":
						_ = unstructured.SetNestedField(object.Object, "/dev/other", "status", "capacityUnit", "source")
					case "copy changed":
						_ = unstructured.SetNestedField(object.Object, "other-copy", "status", "currentCopy", "copyID")
					case "published":
						_ = unstructured.SetNestedStringSlice(object.Object, []string{options.pool.NodeName}, "status", "publishedNodes")
					case "live mount changed":
						refreshUsageObservation(object, registry.Now())
					}
				})
				if waits > 1 {
					t.Fatal("contradiction was treated as retryable staleness")
				}
				return nil
			}
			output, err := measureWithAuthority(context.Background(), registry, options,
				func(string, volumeapi.Pool) error {
					if waits > 0 && scenario == "live mount changed" {
						return errors.New("mount replaced")
					}
					return nil
				},
				func(string, volume.CopyIdentity) error {
					if waits > 0 && scenario == "marker changed" {
						return errors.New("marker replaced")
					}
					return nil
				},
				func(context.Context) (string, error) {
					reads++
					changeMeasurementObject(t, client, volumeapi.PoolResource, options.pool.Name, func(pool *unstructured.Unstructured) { pool.SetGeneration(2) })
					return "123", nil
				})
			if err == nil || output != "" || reads != 1 || waits != 1 {
				t.Fatalf("output=%q err=%v reads=%d waits=%d", output, err, reads, waits)
			}
		})
	}
}

func TestUsageWaitCancellationPublishesNoMeasurement(t *testing.T) {
	registry, client, options := measurementRegistryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	options.wait = func(ctx context.Context) error { cancel(); return waitUsageObservation(ctx) }
	output, err := measureWithAuthority(ctx, registry, options,
		func(string, volumeapi.Pool) error { return nil }, func(string, volume.CopyIdentity) error { return nil },
		func(context.Context) (string, error) {
			reads++
			changeMeasurementObject(t, client, volumeapi.PoolResource, options.pool.Name, func(pool *unstructured.Unstructured) { pool.SetGeneration(2) })
			return "123", nil
		})
	if !errors.Is(err, context.Canceled) || output != "" || reads != 1 {
		t.Fatalf("output=%q err=%v reads=%d", output, err, reads)
	}
}

func TestStatFSStillRejectsGenerationChangeAfterRead(t *testing.T) {
	repo, options := measurementFixture(t)
	options.volumeID, options.copy = "", nil
	options.wait = func(context.Context) error { t.Fatal("statfs entered usage wait"); return nil }
	output, err := measureWithAuthority(context.Background(), repo, options,
		func(string, volumeapi.Pool) error { return nil }, func(string, volume.CopyIdentity) error { return nil },
		func(context.Context) (string, error) { repo.pool.Generation++; return "123", nil })
	if err == nil || output != "" {
		t.Fatalf("statfs accepted stale evidence: %q %v", output, err)
	}
}
