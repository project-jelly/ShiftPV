// Package upgrade observes whether execution responsibilities are settled before
// changing the driver protocol. The snapshot is not atomic: callers must stop
// admitting new work and retain that exclusion until the upgrade finishes.
package upgrade

import (
	"context"
	"fmt"
	"slices"
	"sort"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/history"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type VolumeRepository interface {
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
	ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error)
}

type CleanupRepository interface {
	List(context.Context) ([]cleanupapi.Cleanup, error)
}

type Checker struct {
	Client        kubernetes.Interface
	Volumes       VolumeRepository
	Cleanups      CleanupRepository
	Namespace     string
	NodeDaemonSet string
}

type Blocker struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	Reason    string `json:"reason"`
}

type Report struct {
	Complete bool      `json:"complete"`
	Blockers []Blocker `json:"blockers"`
}

// Safe never treats an incomplete or zero-value observation as approval.
func (r Report) Safe() bool { return r.Complete && len(r.Blockers) == 0 }

type snapshot struct {
	volumes  map[string]volumeapi.State
	moves    []volumeapi.Move
	cleanups []cleanupapi.Cleanup
	pools    []volumeapi.Pool
	jobs     []batchv1.Job
	pods     []corev1.Pod
}

// Check performs only LIST/GET requests. It does not release reservations,
// finalizers or executors, or repair an invalid historical record.
func (c *Checker) Check(ctx context.Context) (Report, error) {
	if c == nil || c.Client == nil || c.Volumes == nil || c.Cleanups == nil ||
		!volume.ValidObjectName(c.Namespace) || !volume.ValidObjectName(c.NodeDaemonSet) {
		return Report{}, fmt.Errorf("upgrade checker configuration is incomplete")
	}
	s, err := c.observe(ctx)
	if err != nil {
		return Report{}, err
	}
	report := Report{Blockers: make([]Blocker, 0)}
	for id, state := range s.volumes {
		if reason := volumeBlocker(id, state); reason != "" {
			report.Blockers = append(report.Blockers, Blocker{Kind: "ShiftPVVolume", Name: id, Reason: reason})
		}
	}
	report.Blockers = append(report.Blockers, historyBlockers(s)...)
	report.Blockers = append(report.Blockers, c.executorBlockers(s)...)
	if err := c.checkCapabilities(ctx, s, &report); err != nil {
		return Report{}, err
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	sort.Slice(report.Blockers, func(i, j int) bool {
		a, b := report.Blockers[i], report.Blockers[j]
		return a.Kind+"\x00"+a.Namespace+"\x00"+a.Name+"\x00"+a.Reason < b.Kind+"\x00"+b.Namespace+"\x00"+b.Name+"\x00"+b.Reason
	})
	report.Complete = true
	return report, nil
}

func (c *Checker) observe(ctx context.Context) (snapshot, error) {
	var s snapshot
	var err error
	if s.volumes, err = c.Volumes.ListVolumes(ctx); err != nil {
		return s, fmt.Errorf("list upgrade Volumes: %w", err)
	}
	if s.moves, err = c.Volumes.ListMoves(ctx); err != nil {
		return s, fmt.Errorf("list upgrade Moves: %w", err)
	}
	if s.cleanups, err = c.Cleanups.List(ctx); err != nil {
		return s, fmt.Errorf("list upgrade cleanup journals: %w", err)
	}
	if s.pools, err = c.Volumes.ListPoolRegistrations(ctx); err != nil {
		return s, fmt.Errorf("list upgrade Pool registrations: %w", err)
	}
	jobs, err := c.Client.BatchV1().Jobs(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return s, fmt.Errorf("list upgrade helper Jobs: %w", err)
	}
	pods, err := c.Client.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return s, fmt.Errorf("list upgrade helper Pods: %w", err)
	}
	s.jobs, s.pods = jobs.Items, pods.Items
	return s, nil
}

func volumeBlocker(id string, state volumeapi.State) string {
	if state.ActiveMove != "" {
		return "ActiveMove"
	}
	if state.Deleting || state.DeletionOperationID != "" || state.HasCleanup {
		return "DeletionOrCleanupUnsettled"
	}
	if state.Phase != volumeapi.PhaseReady {
		return "VolumeNotReady"
	}
	copy := state.CurrentCopy
	if volume.ValidateID(id) != nil || state.UID == "" || state.CapacityBytes <= 0 || copy == nil || copy.Validate() != nil ||
		copy.Role != volume.RoleServing || copy.VolumeID != id || copy.VolumeUID != state.UID || copy.NodeName != state.OwnerNode {
		return "VolumeIdentityInvalid"
	}
	for _, node := range state.PublishedNodes {
		if node != state.OwnerNode {
			return "PublicationOwnerMismatch"
		}
	}
	if !readyCreationSettled(state) {
		return "CreationEvidenceInvalid"
	}
	return ""
}

func readyCreationSettled(state volumeapi.State) bool {
	// Older Ready Volumes need neither a PV nor a native creation receipt.
	if state.CreationExecutor == nil && state.CreationReceipt == nil {
		return true
	}
	if state.CreationExecutor == nil || state.CurrentCopy == nil {
		return false
	}
	// Mobility retains the original creator's receipt after owner transfer.
	// Check its internal creation identity without equating creator and owner.
	copy := *state.CurrentCopy
	copy.NodeName = state.CreationExecutor.NodeName
	state.CurrentCopy = &copy
	return volumeapi.ValidCreationReceipt(state)
}

func historyBlockers(s snapshot) []Blocker {
	var blockers []Blocker
	journals := make(map[cleanupapi.Authority]*cleanupapi.Cleanup)
	for i := range s.cleanups {
		journal := &s.cleanups[i]
		journals[journal.Spec.Authority] = journal
		if err := cleanupapi.ValidateSettled(*journal); err != nil {
			blockers = append(blockers, Blocker{Kind: journal.Spec.Authority.Kind, Name: journal.Spec.Authority.Name, Reason: "CleanupUnsettled: " + err.Error()})
		}
	}
	for _, move := range s.moves {
		var current *volumeapi.State
		if state, exists := s.volumes[move.Spec.VolumeID]; exists {
			current = &state
		}
		authority := cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID}
		if err := history.ValidateSettled(move, current, journals[authority]); err != nil {
			blockers = append(blockers, Blocker{Kind: "ShiftPVMove", Name: move.Name, Reason: "MoveUnsettled: " + err.Error()})
		}
	}
	return blockers
}

func (c *Checker) checkCapabilities(ctx context.Context, s snapshot, report *Report) error {
	nodes := make(map[string]bool)
	for _, pool := range s.pools {
		if pool.DeletionTimestamp == nil && pool.RegistrationApproved() && pool.ConfigurationCheck().OK && volume.ValidObjectName(pool.NodeName) {
			nodes[pool.NodeName] = true
		}
	}
	for _, state := range s.volumes {
		if state.Phase == volumeapi.PhaseReady && volume.ValidObjectName(state.OwnerNode) {
			nodes[state.OwnerNode] = true
		}
	}
	discovery := nodeexecutor.Discovery{Client: c.Client, Namespace: c.Namespace, DaemonSet: c.NodeDaemonSet}
	for node := range nodes {
		executor, err := discovery.FindMoveCleanup(ctx, node)
		if err != nil {
			return fmt.Errorf("observe resident executor on %s: %w", node, err)
		}
		if executor == nil {
			report.Blockers = append(report.Blockers, Blocker{Kind: "Node", Name: node, Reason: "ResidentMoveCleanupUnavailable"})
		}
	}
	return nil
}

type parent struct{ kind, name, uid string }

func (c *Checker) executorBlockers(s snapshot) []Blocker {
	parents := make(map[parent]bool)
	jobUIDs, podUIDs := make(map[string]bool), make(map[string]bool)
	for id, state := range s.volumes {
		parents[parent{"ShiftPVVolume", id, state.UID}] = true
	}
	for _, move := range s.moves {
		parents[parent{"ShiftPVMove", move.Name, move.UID}] = true
	}
	for _, journal := range s.cleanups {
		executor := journal.Status.Executor
		if executor != nil && executor.Kind == "" {
			if executor.JobUID != "" {
				jobUIDs[executor.JobUID] = true
			}
			if executor.PodUID != "" {
				podUIDs[executor.PodUID] = true
			}
		}
	}
	var blockers []Blocker
	for _, job := range s.jobs {
		if !jobUIDs[string(job.UID)] && !ownedByParent(job.OwnerReferences, parents) && !helper(job.Labels, job.Annotations, job.Spec.Template.Spec) {
			continue
		}
		if job.UID != "" {
			jobUIDs[string(job.UID)] = true
		}
		if !terminalJob(job) {
			blockers = append(blockers, Blocker{Kind: "Job", Namespace: job.Namespace, Name: job.Name, Reason: "HelperExecutorPresent"})
		}
	}
	for _, pod := range s.pods {
		if podUIDs[string(pod.UID)] || ownedByParent(pod.OwnerReferences, parents) || ownedByJob(pod.OwnerReferences, jobUIDs) || helper(pod.Labels, pod.Annotations, pod.Spec) {
			if !terminatedPod(pod) {
				blockers = append(blockers, Blocker{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, Reason: "HelperExecutorPresent"})
			}
		}
	}
	return blockers
}

func ownedByParent(owners []metav1.OwnerReference, parents map[parent]bool) bool {
	for _, owner := range owners {
		if owner.UID != "" && owner.APIVersion == "shiftpv.io/v1alpha1" && parents[parent{owner.Kind, owner.Name, string(owner.UID)}] {
			return true
		}
	}
	return false
}

func ownedByJob(owners []metav1.OwnerReference, jobs map[string]bool) bool {
	for _, owner := range owners {
		if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && owner.UID != "" && jobs[string(owner.UID)] {
			return true
		}
	}
	return false
}

func helper(labels, annotations map[string]string, spec corev1.PodSpec) bool {
	if labels["app.kubernetes.io/name"] == "shiftpv" && slices.Contains([]string{"mobility", "volume-helper"}, labels["app.kubernetes.io/component"]) {
		return true
	}
	for _, key := range []string{"shiftpv.io/move-uid", "shiftpv.io/move", "shiftpv.io/cleanup-uid", "shiftpv.io/cleanup-name", "shiftpv.io/source-usage"} {
		if labels[key] != "" {
			return true
		}
	}
	if annotations["shiftpv.io/operation-id"] != "" && annotations["shiftpv.io/volume-uid"] != "" {
		return true
	}
	for _, container := range append(append([]corev1.Container(nil), spec.InitContainers...), spec.Containers...) {
		if slices.Contains(container.Command, "/shiftpv-volume-helper") {
			return true
		}
	}
	return false
}

func terminalJob(job batchv1.Job) bool {
	if job.DeletionTimestamp != nil || job.Status.Active != 0 {
		return false
	}
	complete := false
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Type == batchv1.JobFailed {
			return false
		}
		if condition.Type == batchv1.JobComplete {
			complete = true
		}
	}
	return complete
}

func terminatedPod(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || (pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) || len(pod.Spec.Containers) == 0 {
		return false
	}
	if !containersTerminated(pod.Spec.Containers, pod.Status.ContainerStatuses) || !containersTerminated(pod.Spec.InitContainers, pod.Status.InitContainerStatuses) {
		return false
	}
	var ephemeral []corev1.Container
	for _, container := range pod.Spec.EphemeralContainers {
		ephemeral = append(ephemeral, corev1.Container{Name: container.Name})
	}
	return containersTerminated(ephemeral, pod.Status.EphemeralContainerStatuses)
}

func containersTerminated(containers []corev1.Container, statuses []corev1.ContainerStatus) bool {
	if len(containers) != len(statuses) {
		return false
	}
	for _, container := range containers {
		found := false
		for _, status := range statuses {
			if status.Name == container.Name && status.State.Terminated != nil && status.State.Running == nil && status.State.Waiting == nil {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}
