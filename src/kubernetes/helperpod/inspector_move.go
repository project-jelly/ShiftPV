package helperpod

import (
	"context"
	"crypto/sha256"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// MovesSettled observes one complete namespace snapshot without adopting or
// deleting historical executors. The caller separately validates each Move's
// settled transaction and Volume authority, and must exclude concurrent work.
func (i *Inspector) MovesSettled(ctx context.Context, moves []volumeapi.Move, journals map[string]cleanupapi.Cleanup) error {
	if err := i.validate(); err != nil {
		return err
	}
	if len(moves) == 0 {
		return nil
	}
	if err := inspectorMoveHistoryIdentities(moves, journals); err != nil {
		return err
	}
	jobs, err := i.Client.BatchV1().Jobs(i.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list historical Move Jobs: %w", err)
	}
	if jobs == nil || !inspectorCompleteList(jobs.ListMeta) {
		return fmt.Errorf("historical Move Job observation is incomplete")
	}
	pods, err := i.Client.CoreV1().Pods(i.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list historical Move Pods: %w", err)
	}
	if pods == nil || !inspectorCompleteList(pods.ListMeta) {
		return fmt.Errorf("historical Move Pod observation is incomplete")
	}
	for _, move := range moves {
		var journal *cleanupapi.Cleanup
		if current, exists := journals[move.UID]; exists {
			journal = &current
		}
		if err := i.moveSettled(move, journal, jobs.Items, pods.Items); err != nil {
			return fmt.Errorf("Move %q: %w", move.Name, err)
		}
	}
	return ctx.Err()
}

func inspectorMoveHistoryIdentities(moves []volumeapi.Move, journals map[string]cleanupapi.Cleanup) error {
	identities := make(map[string]string, len(moves))
	names := make(map[string]string, len(moves))
	for _, move := range moves {
		if !inspectorMoveIdentityValid(move) {
			return fmt.Errorf("%w: invalid Move identity", cleanupapi.ErrConflict)
		}
		if identities[move.UID] != "" || names[move.Name] != "" {
			return fmt.Errorf("%w: ambiguous Move name or UID", cleanupapi.ErrConflict)
		}
		identities[move.UID], names[move.Name] = move.Name, move.UID
		for _, name := range []string{move.Status.CopyJobName, move.Status.PromotionJobName} {
			if name != "" && !volume.ValidObjectName(name) {
				return fmt.Errorf("%w: invalid recorded Move Job name", cleanupapi.ErrConflict)
			}
		}
		if current, exists := journals[move.UID]; exists {
			if current.Spec.Authority != (cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID}) || !volume.ValidObjectName(current.Name) {
				return fmt.Errorf("%w: cleanup journal belongs to another Move", cleanupapi.ErrConflict)
			}
			if err := cleanupapi.ValidateSettled(current); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectorMoveIdentityValid(move volumeapi.Move) bool {
	return volume.ValidObjectName(move.Name) && volume.ValidIdentityToken(move.UID) && volume.ValidateID(move.Spec.VolumeID) == nil && volume.ValidObjectName(move.Spec.SourceNode)
}

func inspectorCompleteList(meta metav1.ListMeta) bool {
	return meta.Continue == "" && (meta.RemainingItemCount == nil || *meta.RemainingItemCount == 0)
}

func (i *Inspector) moveSettled(move volumeapi.Move, journal *cleanupapi.Cleanup, jobs []batchv1.Job, pods []corev1.Pod) error {
	jobNames, podNames := inspectorMoveNames(move)
	if journal != nil {
		jobNames[journal.Name+"-effect"] = true
		if journal.Status.Executor.Kind == "" {
			jobNames[journal.Status.Executor.JobName] = true
		}
	}
	ownedJobs, jobsByName, err := inspectorMoveJobs(move, i.Namespace, jobNames, jobs)
	if err != nil {
		return err
	}
	if journal != nil && journal.Status.Executor.Kind == "" {
		job := jobsByName[journal.Status.Executor.JobName]
		if job != nil && !i.sameCleanupExecutor(job, *journal) {
			return fmt.Errorf("%w: completed cleanup Job identity changed", cleanupapi.ErrConflict)
		}
		settled, err := inspectorCleanupPodsSettled(*journal, job == nil, pods, i.Namespace)
		if err != nil {
			return err
		}
		if !settled {
			return fmt.Errorf("historical cleanup executor is not quiescent")
		}
	}
	seenUIDs, seenNames := make(map[string]bool), make(map[string]bool)
	for index := range pods {
		pod := &pods[index]
		if !inspectorMovePod(pod, move, jobNames, podNames, ownedJobs) {
			continue
		}
		if pod.Namespace != i.Namespace || !volume.ValidIdentityToken(string(pod.UID)) || !inspectorMovePodOwner(pod, move, jobsByName, jobNames, podNames) || seenUIDs[string(pod.UID)] || seenNames[pod.Name] {
			return fmt.Errorf("%w: historical Pod %q identity changed or is ambiguous", cleanupapi.ErrConflict, pod.Name)
		}
		seenUIDs[string(pod.UID)], seenNames[pod.Name] = true, true
		if !inspectorPodTerminated(pod) {
			return fmt.Errorf("historical Pod %q is not quiescent", pod.Name)
		}
	}
	return nil
}

func inspectorMoveJobs(move volumeapi.Move, namespace string, names map[string]bool, jobs []batchv1.Job) (map[string]*batchv1.Job, map[string]*batchv1.Job, error) {
	byUID, byName := make(map[string]*batchv1.Job), make(map[string]*batchv1.Job)
	for index := range jobs {
		job := &jobs[index]
		if !names[job.Name] && !inspectorReferencesUID(job.OwnerReferences, move.UID) {
			continue
		}
		if job.Namespace != namespace || !volume.ValidIdentityToken(string(job.UID)) || !inspectorMoveOwner(job, move) || byName[job.Name] != nil || byUID[string(job.UID)] != nil {
			return nil, nil, fmt.Errorf("%w: historical Job %q identity changed or is ambiguous", cleanupapi.ErrConflict, job.Name)
		}
		byUID[string(job.UID)], byName[job.Name] = job, job
		if !inspectorJobComplete(job) {
			return nil, nil, fmt.Errorf("historical Job %q is not quiescent", job.Name)
		}
	}
	return byUID, byName, nil
}

func inspectorMoveNames(move volumeapi.Move) (map[string]bool, map[string]bool) {
	base := inspectorMoveBase(move.Name)
	recovery := inspectorMoveBase(move.Name + "-recovery-" + move.UID)
	jobs := map[string]bool{base + "-copy": true, base + "-promote": true, recovery + "-verify": true, recovery + "-retire": true}
	for _, name := range []string{move.Status.CopyJobName, move.Status.PromotionJobName} {
		if name != "" {
			jobs[name] = true
		}
	}
	pods := map[string]bool{base + "-source": true, base + "-placement": true, sourceUsagePodName(move.UID): true}
	return jobs, pods
}

func inspectorMoveBase(name string) string {
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("shiftpv-move-%x", sum[:6])
}

func inspectorReferencesUID(owners []metav1.OwnerReference, uid string) bool {
	for _, owner := range owners {
		if string(owner.UID) == uid {
			return true
		}
	}
	return false
}

func inspectorMoveOwner(object metav1.Object, move volumeapi.Move) bool {
	owner := metav1.GetControllerOf(object)
	return inspectorSingleController(object) && owner != nil && owner.APIVersion == "shiftpv.io/v1alpha1" && owner.Kind == "ShiftPVMove" && owner.Name == move.Name && string(owner.UID) == move.UID
}

func inspectorSingleController(object metav1.Object) bool {
	controllers := 0
	for _, owner := range object.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller {
			controllers++
		}
	}
	return controllers == 1
}

func inspectorMovePod(pod *corev1.Pod, move volumeapi.Move, jobNames, podNames map[string]bool, ownedJobs map[string]*batchv1.Job) bool {
	if podNames[pod.Name] || inspectorReferencesUID(pod.OwnerReferences, move.UID) || move.Status.PlacementPodUID != "" && string(pod.UID) == move.Status.PlacementPodUID {
		return true
	}
	for _, owner := range pod.OwnerReferences {
		if ownedJobs[string(owner.UID)] != nil || owner.APIVersion == "batch/v1" && owner.Kind == "Job" && jobNames[owner.Name] {
			return true
		}
	}
	return false
}

func inspectorMovePodOwner(pod *corev1.Pod, move volumeapi.Move, jobsByName map[string]*batchv1.Job, jobNames, podNames map[string]bool) bool {
	if move.Status.PlacementPodUID != "" && pod.Name == inspectorMoveBase(move.Name)+"-placement" && string(pod.UID) != move.Status.PlacementPodUID {
		return false
	}
	if podNames[pod.Name] || inspectorReferencesUID(pod.OwnerReferences, move.UID) || move.Status.PlacementPodUID != "" && string(pod.UID) == move.Status.PlacementPodUID {
		return inspectorMoveOwner(pod, move)
	}
	owner := metav1.GetControllerOf(pod)
	if !inspectorSingleController(pod) || owner == nil || owner.APIVersion != "batch/v1" || owner.Kind != "Job" || !volume.ValidIdentityToken(string(owner.UID)) {
		return false
	}
	if job := jobsByName[owner.Name]; job != nil {
		return job.UID == owner.UID
	}
	for _, job := range jobsByName {
		if job.UID == owner.UID {
			return false
		}
	}
	return jobNames[owner.Name]
}
