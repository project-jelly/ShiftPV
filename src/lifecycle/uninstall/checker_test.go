package uninstall

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type memoryRepository struct {
	volumes    map[string]volumeapi.State
	moves      []volumeapi.Move
	pools      []volumeapi.Pool
	removed    []string
	volumesErr error
	movesErr   error
	poolsErr   error
}

type cleanupRepository struct {
	items []cleanupapi.Cleanup
	err   error
}

func TestPoolRemovalProtectsInitializingReservation(t *testing.T) {
	poolA := volumeapi.Pool{Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a"}
	poolB := volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "node-a"}
	state := volumeapi.State{
		UID: "volume-uid", RequestName: "pvc", InitialNode: "node-a", CapacityBytes: 64,
		Finalizers:  []string{volumeapi.VolumeProtectionFinalizer},
		InitialPool: &volumeapi.PoolReference{Name: poolA.Name, UID: poolA.UID},
	}
	assert := func(pool volumeapi.Pool, want int) {
		t.Helper()
		if got := poolVolumeBlockers(map[string]volumeapi.State{"v": state}, pool); len(got) != want {
			t.Fatalf("pool=%s blockers=%+v want=%d", pool.Name, got, want)
		}
	}
	assert(poolA, 1)
	assert(poolB, 0)
	state.InitialPool = nil // Old unassigned intents conservatively protect their node.
	assert(poolA, 1)
	assert(poolB, 1)
	assert(volumeapi.Pool{Name: "other", UID: "other-uid", NodeName: "node-b"}, 0)
}

func (r cleanupRepository) List(context.Context) ([]cleanupapi.Cleanup, error) { return r.items, r.err }

func TestCheckBlocksUnsettledCleanupContract(t *testing.T) {
	items := []cleanupapi.Cleanup{
		{Name: "volume-journal", Spec: cleanupapi.Spec{OperationID: "cleanup-a", Target: volume.CopyIdentity{VolumeID: "shiftpv-0123456789abcdef0123456789abcdef"}, Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: "shiftpv-0123456789abcdef0123456789abcdef"}}, Status: cleanupapi.Status{Phase: cleanupapi.PhaseRunning}},
		{Name: "move-journal", Spec: cleanupapi.Spec{OperationID: "cleanup-b", Target: volume.CopyIdentity{VolumeID: "shiftpv-fedcba9876543210fedcba9876543210"}, Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move-a"}}, Status: cleanupapi.Status{Phase: cleanupapi.PhaseVerifying}},
		{Name: "settled", Status: cleanupapi.Status{Phase: cleanupapi.PhaseCompleted}},
	}
	checker := &Checker{Client: fake.NewClientset(), Volumes: &memoryRepository{}, Cleanups: cleanupRepository{items: items}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	report, err := checker.Check(context.Background())
	if err != nil || len(report.Blockers) != 2 || report.Blockers[0].Kind != "ShiftPVMove" || report.Blockers[0].Name != "move-a" || report.Blockers[1].Kind != "ShiftPVVolume" {
		t.Fatalf("cleanup blockers=%#v err=%v", report.Blockers, err)
	}
	checker.Cleanups = cleanupRepository{err: errors.New("cleanup api unavailable")}
	if _, err := checker.Check(context.Background()); err == nil {
		t.Fatal("cleanup API failure did not close uninstall gate")
	}
	checker.Cleanups = cleanupRepository{items: []cleanupapi.Cleanup{{Name: "invalid-parent", Status: cleanupapi.Status{Phase: cleanupapi.PhaseRunning}}}}
	if _, err := checker.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid parent authority") {
		t.Fatalf("invalid embedded cleanup journal did not close uninstall gate: %v", err)
	}
}

func (m *memoryRepository) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	return m.volumes, m.volumesErr
}

func (m *memoryRepository) ListMoves(context.Context) ([]volumeapi.Move, error) {
	return m.moves, m.movesErr
}

func (m *memoryRepository) ListPools(context.Context) ([]volumeapi.Pool, error) {
	return m.pools, m.poolsErr
}

func (m *memoryRepository) ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error) {
	return m.pools, m.poolsErr
}

func (m *memoryRepository) RemovePoolFinalizer(_ context.Context, name, uid string) error {
	m.removed = append(m.removed, name+"/"+uid)
	return nil
}

func TestCheckAllowsEmptyCluster(t *testing.T) {
	checker := &Checker{Client: fake.NewClientset(), Volumes: &memoryRepository{}, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	report, err := checker.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !report.Safe() {
		t.Fatalf("Check() blockers = %#v", report.Blockers)
	}
}

func TestReleasePoolProtectionUsesExactRegisteredIdentity(t *testing.T) {
	repository := &memoryRepository{pools: []volumeapi.Pool{
		{Name: "protected", UID: "protected-uid", Finalizers: []string{PoolProtectionFinalizer}},
		{Name: "plain", UID: "plain-uid"},
	}}
	checker := &Checker{Volumes: repository}
	if err := checker.ReleasePoolProtection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repository.removed) != 1 || repository.removed[0] != "protected/protected-uid" {
		t.Fatalf("removed=%v", repository.removed)
	}
}

func TestCheckPoolDeleteAllowsExactEmptyPoolWhileOtherPoolsRemainActive(t *testing.T) {
	now := time.Date(2026, time.September, 12, 7, 0, 0, 0, time.UTC)
	poolA := volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1,
			Conditions: []metav1.Condition{{Type: volumeapi.PoolConditionIdentityReleased, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "PoolIdentityReleased"}},
			Inventory:  &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}},
	}
	poolB := volumeapi.Pool{
		Name: "pool-b", UID: "pool-b-uid", NodeName: "node-b", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}},
	}
	otherCopy := volume.CopyIdentity{PoolName: poolB.Name, PoolUID: poolB.UID, NodeName: poolB.NodeName}
	storageClassName := "shiftpv"
	client := fake.NewClientset(
		shiftPVPersistentVolumeOnNode("pv-other", poolB.NodeName),
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClassName, VolumeName: "pv-other"}},
	)
	repository := &memoryRepository{
		pools:   []volumeapi.Pool{poolA, poolB},
		volumes: map[string]volumeapi.State{"other": {OwnerNode: poolB.NodeName, CurrentCopy: &otherCopy}},
		moves:   []volumeapi.Move{{Name: "other", Spec: volumeapi.MoveSpec{SourceNode: poolB.NodeName}, Status: volumeapi.MoveStatus{Phase: "Copying", DestinationPoolUID: poolB.UID}}},
	}
	cleanups := cleanupRepository{items: []cleanupapi.Cleanup{{Name: "other", Spec: cleanupapi.Spec{Target: volume.CopyIdentity{PoolName: poolB.Name, PoolUID: poolB.UID}}}}}
	checker := &Checker{Client: client, Volumes: repository, Cleanups: cleanups, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system", Now: func() time.Time { return now }}

	report, err := checker.CheckPoolDeleteAfter(context.Background(), poolA.Name, types.UID(poolA.UID), now.Add(-time.Second))
	if err != nil || !report.Safe() {
		t.Fatalf("empty Pool deletion blockers=%#v err=%v", report.Blockers, err)
	}
	report, err = checker.CheckPoolDeleteAfter(context.Background(), poolA.Name, types.UID(poolA.UID), now)
	if err != nil || !report.WaitingForInventory() {
		t.Fatalf("pre-delete Pool inventory report=%#v err=%v", report.Blockers, err)
	}
}

func TestCheckPoolDeleteAllowsEmptyDuplicateWhenLiveVolumeIdentifiesOtherPool(t *testing.T) {
	now := time.Date(2026, time.September, 12, 10, 0, 0, 0, time.UTC)
	target := volumeapi.Pool{
		Name: "pool-empty-duplicate", UID: "pool-empty-uid", NodeName: "node-a", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1,
			Conditions: []metav1.Condition{{Type: volumeapi.PoolConditionIdentityReleased, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "PoolIdentityReleased"}},
			Inventory:  &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}},
	}
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	currentCopy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-surviving", PoolUID: "pool-surviving-uid", VolumeID: volumeID,
		VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: target.NodeName, Role: volume.RoleServing,
	}
	destinationVolumeID := "shiftpv-fedcba9876543210fedcba9876543210"
	destinationCopy := volume.CopyIdentity{
		InstallationID: currentCopy.InstallationID, PoolName: currentCopy.PoolName, PoolUID: currentCopy.PoolUID,
		VolumeID: destinationVolumeID, VolumeUID: "destination-volume-uid", CopyID: "destination-copy-id",
		NodeName: target.NodeName, Role: volume.RoleServing,
	}
	client := fake.NewClientset(shiftPVPersistentVolumeOnNode(volumeID, target.NodeName))
	repository := &memoryRepository{
		pools: []volumeapi.Pool{target, {Name: currentCopy.PoolName, UID: currentCopy.PoolUID, NodeName: target.NodeName}},
		volumes: map[string]volumeapi.State{volumeID: {
			UID: currentCopy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: target.NodeName, CurrentCopy: &currentCopy, PublishedNodes: []string{target.NodeName},
		}},
		moves: []volumeapi.Move{
			{Name: "move-source", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: target.NodeName}, Status: volumeapi.MoveStatus{Phase: "Copying", SourceCopy: &currentCopy}},
			{Name: "move-destination", Spec: volumeapi.MoveSpec{VolumeID: destinationVolumeID, SourceNode: "node-source"}, Status: volumeapi.MoveStatus{
				Phase: "Copying", DestinationNode: target.NodeName, DestinationPoolUID: destinationCopy.PoolUID,
				CandidateNodes: []string{target.NodeName}, DestinationCopy: &destinationCopy,
			}},
		},
	}
	checker := &Checker{Client: client, Volumes: repository, Cleanups: cleanupRepository{}, Namespace: "shiftpv-system", Now: func() time.Time { return now }}
	report, err := checker.CheckPoolDeleteAfter(context.Background(), target.Name, types.UID(target.UID), now.Add(-time.Second))
	if err != nil || !report.Safe() {
		t.Fatalf("exact other-Pool ownership blocked empty duplicate deletion: blockers=%#v err=%v", report.Blockers, err)
	}
}

func TestCheckPoolDeleteBlocksEveryTargetPoolDependency(t *testing.T) {
	now := time.Date(2026, time.September, 12, 7, 0, 0, 0, time.UTC)
	target := volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{
			ObservedAt: metav1.NewTime(now), Valid: true, Copies: []volumeapi.CopyObservation{{Marker: "copy.json", Present: true}},
		}},
	}
	currentCopy := volume.CopyIdentity{PoolName: target.Name, PoolUID: target.UID, NodeName: target.NodeName}
	client := fake.NewClientset(shiftPVPersistentVolumeOnNode("pv-data", target.NodeName))
	foreignCopy := volume.CopyIdentity{PoolName: "pool-b", PoolUID: "pool-b-uid", NodeName: "node-b"}
	repository := &memoryRepository{
		pools: []volumeapi.Pool{target},
		volumes: map[string]volumeapi.State{
			"data":              {OwnerNode: target.NodeName, CurrentCopy: &currentCopy},
			"foreign-published": {OwnerNode: "node-b", CurrentCopy: &foreignCopy, PublishedNodes: []string{target.NodeName}},
		},
		moves: []volumeapi.Move{{
			Name: "move-data", Spec: volumeapi.MoveSpec{VolumeID: "data", SourceNode: target.NodeName}, Status: volumeapi.MoveStatus{Phase: "Copying", SourceCopy: &currentCopy},
		}},
	}
	cleanups := cleanupRepository{items: []cleanupapi.Cleanup{{
		Name: "cleanup-data", Spec: cleanupapi.Spec{OperationID: "cleanup-operation", Target: currentCopy, Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: "data"}}, Status: cleanupapi.Status{Phase: cleanupapi.PhaseRunning},
	}}}
	checker := &Checker{Client: client, Volumes: repository, Cleanups: cleanups, Namespace: "shiftpv-system", Now: func() time.Time { return now }}

	report, err := checker.CheckPoolDeleteAfter(context.Background(), target.Name, types.UID(target.UID), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	joined := blockersText(report.Blockers)
	for _, expected := range []string{"PersistentVolume//pv-data/", "ShiftPVMove//move-data/", "ShiftPVPoolCopy//pool-a/", "ShiftPVVolume//data/cleanupPhase=Running operation=cleanup-operation", "ShiftPVVolume//foreign-published/"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("Pool blockers do not contain %q: %s", expected, joined)
		}
	}

	if _, err := checker.CheckPoolDeleteAfter(context.Background(), target.Name, "replacement-uid", time.Time{}); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("Pool UID mismatch error = %v", err)
	}
}

func TestCheckPoolDeleteFencesMoveCandidateBeforeCapacityAdmission(t *testing.T) {
	now := time.Date(2026, time.September, 12, 9, 0, 0, 0, time.UTC)
	target := volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}},
	}
	repository := &memoryRepository{pools: []volumeapi.Pool{target}, moves: []volumeapi.Move{{
		Name: "move-pending-capacity", Spec: volumeapi.MoveSpec{VolumeID: "volume", SourceNode: "node-source"},
		Status: volumeapi.MoveStatus{Phase: "WaitingForCapacity", CandidateNodes: []string{target.NodeName}},
	}}}
	checker := &Checker{Client: fake.NewClientset(), Volumes: repository, Cleanups: cleanupRepository{}, Namespace: "shiftpv-system", Now: func() time.Time { return now }}
	report, err := checker.CheckPoolDeleteAfter(context.Background(), target.Name, types.UID(target.UID), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if text := blockersText(report.Blockers); !strings.Contains(text, "ShiftPVMove//move-pending-capacity/") {
		t.Fatalf("candidate move did not fence Pool deletion: %s", text)
	}
}

func TestCheckPoolDeleteKeepsUnsettledBlockedMoveDependency(t *testing.T) {
	now := time.Date(2026, time.September, 12, 9, 30, 0, 0, time.UTC)
	target := volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}},
	}
	move := volumeapi.Move{
		Name: "move-blocked", Spec: volumeapi.MoveSpec{VolumeID: "volume", SourceNode: "node-source"},
		Status: volumeapi.MoveStatus{
			Phase: "Blocked", DestinationNode: target.NodeName, DestinationPoolUID: target.UID,
			CapacityApproved: true,
		},
	}
	repository := &memoryRepository{pools: []volumeapi.Pool{target}, moves: []volumeapi.Move{move}}
	checker := &Checker{Client: fake.NewClientset(), Volumes: repository, Cleanups: cleanupRepository{}, Namespace: "shiftpv-system", Now: func() time.Time { return now }}

	report, err := checker.CheckPoolDeleteAfter(context.Background(), target.Name, types.UID(target.UID), time.Time{})
	if err != nil || report.Safe() || !strings.Contains(blockersText(report.Blockers), "ShiftPVMove//move-blocked/") {
		t.Fatalf("unsettled blocked Move did not retain Pool protection: report=%#v err=%v", report, err)
	}

	repository.moves[0].Status.RecoveryPhase = "Recovered"
	repository.moves[0].Status.CapacityApproved = false
	repository.moves[0].Status.CapacityReason = "RecoverySettled"
	report, err = checker.CheckPoolDeleteAfter(context.Background(), target.Name, types.UID(target.UID), time.Time{})
	if err != nil || !report.Safe() {
		t.Fatalf("settled recovered Move still blocked empty Pool deletion: report=%#v err=%v", report, err)
	}
}

func shiftPVPersistentVolumeOnNode(name, nodeName string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: name}},
		NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: topologyKey, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}}},
		}}}},
	}}
}

func TestCheckIgnoresLegacyReservationConfigMaps(t *testing.T) {
	legacyReservation := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "shiftpv-a", Namespace: "shiftpv-system", Labels: map[string]string{
		"app.kubernetes.io/name": "shiftpv", "app.kubernetes.io/component": "volume-reservation",
	}}}
	client := fake.NewClientset(legacyReservation)
	checker := &Checker{Client: client, Volumes: &memoryRepository{}, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}

	report, err := checker.Check(context.Background())
	if err != nil || !report.Safe() {
		t.Fatalf("legacy reservation affected uninstall: %#v err=%v", report.Blockers, err)
	}
}

func TestCheckBlocksUnfinishedCompletionWithoutVolumeLock(t *testing.T) {
	checker := &Checker{Client: fake.NewClientset(), StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system", Cleanups: cleanupRepository{}, Volumes: &memoryRepository{
		moves: []volumeapi.Move{{Name: "finishing", Spec: volumeapi.MoveSpec{VolumeID: "volume"}, Status: volumeapi.MoveStatus{Phase: "Completing"}}},
	}}
	report, err := checker.Check(context.Background())
	if err != nil || report.Safe() || len(report.Blockers) != 1 || report.Blockers[0].Kind != "ShiftPVMove" {
		t.Fatalf("unfinished completion did not block uninstall: %+v, %v", report, err)
	}
}

func TestCheckReportsEveryShiftPVDependency(t *testing.T) {
	storageClassName := "shiftpv"
	client := fake.NewClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-data"}, Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: "shiftpv-a"}},
			ClaimRef:               &corev1.ObjectReference{Namespace: "app", Name: "data"},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "app"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClassName, VolumeName: "pv-data"}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-other"}, Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "other.csi.example", VolumeHandle: "other"}},
		}},
	)
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{"shiftpv-a": {Phase: volumeapi.PhaseMoving, OwnerNode: "node-a", ActiveMove: "move-a", PublishedNodes: []string{"node-a"}}},
		moves: []volumeapi.Move{
			{Name: "move-a", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-a"}, Status: volumeapi.MoveStatus{Phase: "Copying"}},
			{Name: "move-complete", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-b"}, Status: volumeapi.MoveStatus{Phase: "Succeeded", CleanupPhase: "Completed"}},
			{Name: "move-blocked", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-c"}, Status: volumeapi.MoveStatus{Phase: "Blocked", RecoveryPhase: "Recovered", CapacityReason: "RecoverySettled"}},
			{Name: "move-blocked-unsettled", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-d"}, Status: volumeapi.MoveStatus{Phase: "Blocked", CapacityApproved: true}},
		},
	}

	report, err := (&Checker{Client: client, Volumes: repository, Cleanups: cleanupRepository{}, StorageClassNames: []string{storageClassName}, Namespace: "shiftpv-system"}).Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if report.Safe() {
		t.Fatal("Check() unexpectedly allowed uninstall")
	}
	if len(report.Blockers) != 5 {
		t.Fatalf("len(blockers) = %d, want 5: %#v", len(report.Blockers), report.Blockers)
	}
	joined := blockersText(report.Blockers)
	for _, expected := range []string{
		"PersistentVolume//pv-data/driver=csi.shiftpv.io volumeHandle=shiftpv-a claim=app/data",
		"PersistentVolumeClaim/app/data/references the ShiftPV StorageClass volume=pv-data",
		"ShiftPVMove//move-a/phase=Copying volume=shiftpv-a",
		"ShiftPVMove//move-blocked-unsettled/phase=Blocked volume=shiftpv-d",
		"ShiftPVVolume//shiftpv-a/phase=Moving owner=node-a activeMove=move-a publishedNodes=node-a",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("blockers do not contain %q: %s", expected, joined)
		}
	}
	if strings.Contains(joined, "pv-other") || strings.Contains(joined, "move-complete") || strings.Contains(joined, "ShiftPVMove//move-blocked/") {
		t.Fatalf("blockers include unrelated or terminal resources: %s", joined)
	}
}

func TestCheckBlocksPendingClaimForEveryConfiguredStorageClass(t *testing.T) {
	retainClass := "shiftpv-retain"
	otherClass := "other"
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "app"},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &retainClass},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &otherClass},
		},
	)
	checker := &Checker{
		Client:            client,
		Volumes:           &memoryRepository{},
		Cleanups:          cleanupRepository{},
		StorageClassNames: []string{"shiftpv", "shiftpv-retain"},
		Namespace:         "shiftpv-system",
	}

	report, err := checker.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Blockers) != 1 || report.Blockers[0].Kind != "PersistentVolumeClaim" || report.Blockers[0].Name != "retained" {
		t.Fatalf("unexpected blockers: %#v", report.Blockers)
	}
}

func TestCheckFailsClosedOnAPIError(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("list", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api unavailable")
	})
	checker := &Checker{Client: client, Volumes: &memoryRepository{}, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	_, err := checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "api unavailable") {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestCheckDoesNotListLegacyReservations(t *testing.T) {
	client := fake.NewClientset()
	listed := false
	client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		listed = true
		return true, nil, errors.New("reservation API unavailable")
	})
	checker := &Checker{Client: client, Volumes: &memoryRepository{}, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	report, err := checker.Check(context.Background())
	if err != nil || !report.Safe() || listed {
		t.Fatalf("legacy reservation API was consulted: report=%#v err=%v listed=%t", report, err, listed)
	}
}

func TestCheckFailsClosedOnRepositoryError(t *testing.T) {
	checker := &Checker{Client: fake.NewClientset(), Volumes: &memoryRepository{volumesErr: errors.New("crd unavailable")}, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	_, err := checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "crd unavailable") {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestCheckBlocksPhysicalPoolCopiesAndUncertainInventory(t *testing.T) {
	now := time.Date(2026, time.September, 12, 5, 0, 0, 0, time.UTC)
	identity := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef",
		VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	repository := &memoryRepository{pools: []volumeapi.Pool{{
		Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/var/lib/shiftpv", Generation: 2,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{
			ObservedAt: metav1.NewTime(now.Add(-4 * time.Minute)), Valid: false, Truncated: true, Message: "CopyObservationProblem",
			Copies: []volumeapi.CopyObservation{{Marker: "copy-copy-id.json", Identity: &identity, Present: true, Published: true}},
		}},
	}}}
	checker := &Checker{
		Client: fake.NewClientset(), Volumes: repository, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system",
		Now: func() time.Time { return now },
	}

	report, err := checker.Check(context.Background())
	if err != nil || report.Safe() || len(report.Blockers) != 1 || !report.WaitingForInventory() {
		t.Fatalf("unsafe Pool inventory report=%#v err=%v", report, err)
	}
	joined := blockersText(report.Blockers)
	expected := "ShiftPVPoolInventory//pool-a/generation=2 observedGeneration=1 valid=false truncated=true message=CopyObservationProblem observedAt=stale"
	if !strings.Contains(joined, expected) {
		t.Fatalf("Pool inventory blockers do not contain %q: %s", expected, joined)
	}
	if strings.Contains(joined, "ShiftPVPoolCopy") {
		t.Fatalf("untrustworthy inventory emitted a copy blocker: %s", joined)
	}
}

func TestCheckRequiresEmptyInventoryObservedAfterQuiesce(t *testing.T) {
	now := time.Date(2026, time.September, 12, 5, 0, 0, 0, time.UTC)
	pool := volumeapi.Pool{
		Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/var/lib/shiftpv", Generation: 1,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now.Add(-time.Second)), Valid: true}},
	}
	repository := &memoryRepository{pools: []volumeapi.Pool{pool}}
	checker := &Checker{
		Client: fake.NewClientset(), Volumes: repository, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system",
		Now: func() time.Time { return now },
	}

	report, err := checker.CheckAfter(context.Background(), now)
	if err != nil || report.Safe() || !report.WaitingForInventory() {
		t.Fatalf("pre-quiesce inventory report=%#v err=%v", report, err)
	}
	repository.pools[0].Status.Inventory.Copies = []volumeapi.CopyObservation{{Marker: "copy-stale.json", Present: true}}
	report, err = checker.CheckAfter(context.Background(), now)
	if err != nil || report.Safe() || !report.WaitingForInventory() || len(report.Blockers) != 1 {
		t.Fatalf("pre-quiesce copy inventory report=%#v err=%v", report, err)
	}
	repository.pools[0].Status.Inventory.ObservedAt = metav1.NewTime(now.Add(time.Second))
	repository.pools[0].Status.Inventory.Copies = nil
	checker.Now = func() time.Time { return now.Add(2 * time.Second) }
	report, err = checker.CheckAfter(context.Background(), now)
	if err != nil || !report.Safe() || report.WaitingForInventory() {
		t.Fatalf("post-quiesce empty inventory report=%#v err=%v", report, err)
	}

	repository.poolsErr = errors.New("Pool API unavailable")
	if _, err := checker.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "Pool API unavailable") {
		t.Fatalf("Pool API failure did not close uninstall gate: %v", err)
	}
}

func TestCheckWaitsForDeletingPoolIdentityRelease(t *testing.T) {
	now := time.Date(2026, time.September, 12, 10, 0, 0, 0, time.UTC)
	deletedAt := metav1.NewTime(now.Add(-time.Minute))
	pool := volumeapi.Pool{
		Name: "pool-a", UID: "pool-uid", NodeName: "node-a", Generation: 1, DeletionTimestamp: &deletedAt,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}},
	}
	repository := &memoryRepository{pools: []volumeapi.Pool{pool}}
	checker := &Checker{
		Client: fake.NewClientset(), Volumes: repository, Cleanups: cleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system",
		Now: func() time.Time { return now },
	}
	report, err := checker.CheckAfter(context.Background(), deletedAt.Time)
	if err != nil || report.Safe() || !report.WaitingForInventory() || len(report.Blockers) != 1 || report.Blockers[0].Kind != "ShiftPVPoolIdentity" {
		t.Fatalf("retained Pool identity report=%#v err=%v", report, err)
	}
	repository.pools[0].Status.Conditions = []metav1.Condition{{
		Type: volumeapi.PoolConditionIdentityReleased, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "PoolIdentityReleased",
	}}
	report, err = checker.CheckAfter(context.Background(), deletedAt.Time)
	if err != nil || !report.Safe() {
		t.Fatalf("released Pool identity report=%#v err=%v", report, err)
	}
}

func TestCheckValidatesConfiguration(t *testing.T) {
	_, err := (&Checker{}).Check(context.Background())
	if err == nil {
		t.Fatal("Check() unexpectedly accepted an empty configuration")
	}

	checker := &Checker{Client: fake.NewClientset(), Volumes: &memoryRepository{}, Cleanups: cleanupRepository{}, Namespace: "shiftpv-system"}
	_, err = checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "StorageClass name") {
		t.Fatalf("Check() empty StorageClass error = %v", err)
	}

	checker.StorageClassNames = []string{"shiftpv"}
	checker.Namespace = ""
	report, err := checker.Check(context.Background())
	if err != nil || !report.Safe() {
		t.Fatalf("Check() retained a reservation namespace dependency: report=%#v err=%v", report, err)
	}
}

func blockersText(blockers []Blocker) string {
	var lines []string
	for _, blocker := range blockers {
		lines = append(lines, blocker.Kind+"/"+blocker.Namespace+"/"+blocker.Name+"/"+blocker.Reason)
	}
	return strings.Join(lines, "\n")
}
