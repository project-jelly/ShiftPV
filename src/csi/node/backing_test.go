package node

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

func TestExistingCopyPublishIgnoresPeerAdmissionFailureButRechecksBacking(t *testing.T) {
	for _, scenario := range []string{"peer unavailable", "own backing changed", "own backing changed under lock"} {
		t.Run(scenario, func(t *testing.T) {
			binder := &fakeBinder{}
			s, pool, deps := fixedPoolService(t, binder)
			pool.Status.Conditions[0].Status = metav1.ConditionFalse
			pool.Status.Conditions[0].Reason = "CapacityPeerUnproven"
			pool.Status.Conditions = append(pool.Status.Conditions, metav1.Condition{
				Type: volumeapi.PoolConditionCapacityBackingVerified, Status: metav1.ConditionTrue, ObservedGeneration: 1,
			})
			checks := 0
			deps.Allocations = changingAllocation(func(string) ([]capacityunit.Extent, error) {
				checks++
				if scenario == "own backing changed" || scenario == "own backing changed under lock" && checks > 1 {
					return []capacityunit.Extent{{Device: "8:0", Start: 8192, Sectors: 4096}}, nil
				}
				return pool.Status.CapacityUnit.Extents, nil
			})
			setPoolProbe(t, s, *deps)
			s.Pools = fakePoolRegistry{pool: pool}
			_, err := s.NodePublishVolume(context.Background(), validPublishRequest())
			if scenario == "peer unavailable" {
				if err != nil || binder.publishedTarget == "" || checks < 2 {
					t.Fatalf("existing owner publish blocked: checks=%d target=%q err=%v", checks, binder.publishedTarget, err)
				}
			} else if err == nil || binder.publishedTarget != "" {
				t.Fatalf("changed backing was mounted: checks=%d target=%q err=%v", checks, binder.publishedTarget, err)
			}
		})
	}
}
