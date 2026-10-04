package volumeapi

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRequiredMountPointReadinessRequiresRecordedIdentityAndMountCondition(t *testing.T) {
	now := time.Now().UTC()
	pool := Pool{Generation: 1, MountPolicy: PoolMountPolicyRequireMountPoint, Status: PoolStatus{
		ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now),
		MountIdentity: &PoolMountIdentity{Device: "8:2", Root: "/", Source: "/dev/disk-a", Filesystem: "ext4"},
		Conditions: []metav1.Condition{
			{Type: PoolConditionMounted, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "MountVerified"},
			{Type: PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "PoolReady"},
		},
	}}
	if ready, reason := pool.ReadyAt(now, time.Minute); !ready || reason != "PoolReady" {
		t.Fatalf("valid mounted Pool: ready=%t reason=%s", ready, reason)
	}
	pool.Status.MountIdentity = nil
	if ready, reason := pool.ReadyAt(now, time.Minute); ready || reason != "MountIdentityMissing" {
		t.Fatalf("missing mount identity: ready=%t reason=%s", ready, reason)
	}
	pool.Status.MountIdentity = &PoolMountIdentity{Device: "8:2", Root: "/", Source: "/dev/disk-a", Filesystem: "ext4"}
	pool.Status.Conditions[0].Status = metav1.ConditionFalse
	pool.Status.Conditions[0].Reason = "MountMissing"
	if ready, reason := pool.ReadyAt(now, time.Minute); ready || reason != "MountMissing" {
		t.Fatalf("missing mount: ready=%t reason=%s", ready, reason)
	}
}
