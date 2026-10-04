package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func (r *Reconciler) quiesceMove(ctx context.Context, move volumeapi.Move) (bool, error) {
	names := namesFor(move.Name)
	quiet := true
	for _, name := range []string{names.CopyJob, names.PromotionJob} {
		gone, err := r.removeJob(ctx, name, names.Base, move.UID)
		if err != nil {
			return false, err
		}
		quiet = quiet && gone
	}
	// List includes orphaned helper Pods, not just the source daemon. No forced
	// deletion: API disappearance is only trusted with healthy participating nodes.
	pods, err := r.Client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "shiftpv.io/move=" + names.Base})
	if err != nil {
		return false, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !moveOwned(pod.OwnerReferences, move.UID) || pod.Labels["shiftpv.io/move-uid"] != move.UID {
			return false, fmt.Errorf("helper Pod %q is not owned by ShiftPVMove UID %q", pod.Name, move.UID)
		}
		placement := pod.Name == names.PlacementPod && pod.Labels["shiftpv.io/role"] == placementRole
		if placement {
			if err := validatePlacementIdentity(pod, move, names); err != nil {
				return false, err
			}
		}
		// A placement Pod only reserves scheduler capacity and never mounts or
		// mutates volume data. It may already be scheduled before the controller
		// durably records DestinationNode, so recovery can safely remove the exact
		// Move-owned reservation regardless of its assigned node. Every data helper
		// remains restricted to a recorded source or destination.
		if !placement && pod.Spec.NodeName != move.Spec.SourceNode && (move.Status.DestinationNode == "" || pod.Spec.NodeName != move.Status.DestinationNode) {
			return false, fmt.Errorf("helper Pod %q has unrecorded node %q", pod.Name, pod.Spec.NodeName)
		}
		quiet = false
		if pod.DeletionTimestamp == nil {
			uid := pod.UID
			if err := r.Client.CoreV1().Pods(r.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
		}
	}
	if !quiet {
		return false, nil
	}
	if err := r.deleteTransferResources(ctx, move, names); err != nil {
		return false, err
	}
	return true, nil
}

func recoveryNames(move volumeapi.Move) resourceNames {
	return namesFor(move.Name + "-recovery-" + move.UID)
}

// verifyOwnerJob builds the single recovery owner-verification Job spec used
// both to create the Job and to compare an existing one against it.
func (r *Reconciler) verifyOwnerJob(ctx context.Context, move volumeapi.Move, names resourceNames, name, node string) (*batchv1.Job, error) {
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		return nil, err
	}
	if state.CurrentCopy == nil || state.CurrentCopy.NodeName != node || state.CurrentCopy.Role != "Serving" {
		return nil, fmt.Errorf("recovery owner copy identity is missing")
	}
	job, err := r.operationJob(ctx, name, state.CurrentCopy, names, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	controller := true
	job.OwnerReferences = []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.Name, UID: types.UID(move.UID), Controller: &controller}}
	job.Labels["shiftpv.io/move-uid"] = move.UID
	job.Spec.Template.Labels["shiftpv.io/move-uid"] = move.UID
	job.Spec.Template.Spec.Containers[0].Command = []string{"/shiftpv-volume-helper"}
	job.Spec.Template.Spec.Containers[0].Args = []string{
		"verify-owner", "--move-name=" + move.Name, "--move-uid=" + move.UID,
		"--operation-id=verify-" + move.UID, "--namespace=" + r.Namespace,
	}
	job.Spec.Template.Spec.Containers[0].Env = podNameEnvironment()
	// Preserve completion evidence across long controller outages. Only the
	// recovery controller removes these Jobs after durable phase advancement.
	job.Spec.TTLSecondsAfterFinished = nil
	zeroRetries := int32(0)
	job.Spec.BackoffLimit = &zeroRetries
	job.Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
	return job, nil
}

func (r *Reconciler) recoveryJob(ctx context.Context, move volumeapi.Move) (bool, error) {
	names := recoveryNames(move)
	node := move.Status.RecoveryOwner
	name := names.Base + "-verify"
	job, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		job, err = r.verifyOwnerJob(ctx, move, names, name, node)
		if err != nil {
			return false, err
		}
		_, err = r.Client.BatchV1().Jobs(r.Namespace).Create(ctx, job, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			err = nil
		}
		return false, err
	}
	if err != nil {
		return false, err
	}
	if job.Labels["shiftpv.io/move"] != names.Base || !moveOwned(job.OwnerReferences, move.UID) || job.Spec.Template.Spec.NodeName != node || job.DeletionTimestamp != nil {
		return false, fmt.Errorf("recovery Job %q identity or node mismatch", name)
	}
	expected, buildErr := r.verifyOwnerJob(ctx, move, names, name, node)
	if buildErr != nil {
		return false, buildErr
	}
	if !sameOperationJob(job, expected) {
		return false, fmt.Errorf("recovery Job %q execution identity changed", name)
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Type == batchv1.JobFailed {
			return false, fmt.Errorf("recovery Job %q failed; inspect logs and storage before retry", name)
		}
		if condition.Type == batchv1.JobComplete {
			return true, nil
		}
	}
	return false, nil
}

func (r *Reconciler) removeRecoveryJobs(ctx context.Context, move volumeapi.Move) (bool, error) {
	names := recoveryNames(move)
	quiet := true
	for _, name := range []string{names.Base + "-verify", names.Base + "-retire"} {
		gone, err := r.removeJob(ctx, name, names.Base, move.UID)
		if err != nil {
			return false, err
		}
		quiet = quiet && gone
	}
	pods, err := r.Client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "shiftpv.io/move=" + names.Base})
	if err != nil {
		return false, err
	}
	return quiet && len(pods.Items) == 0, nil
}

func (r *Reconciler) removeJob(ctx context.Context, name, label, moveUID string) (bool, error) {
	job, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if job.Labels["shiftpv.io/move"] != label || (moveUID != "" && !moveOwned(job.OwnerReferences, moveUID)) {
		return false, fmt.Errorf("refusing to delete unrelated Job %q", name)
	}
	if job.DeletionTimestamp == nil {
		uid := job.UID
		foreground := metav1.DeletePropagationForeground
		err = r.Client.BatchV1().Jobs(r.Namespace).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: &foreground})
		if apierrors.IsNotFound(err) {
			err = nil
		}
	}
	return false, err
}

func moveOwned(references []metav1.OwnerReference, uid string) bool {
	if uid == "" {
		return false
	}
	for _, ref := range references {
		if ref.Kind == "ShiftPVMove" && ref.APIVersion == "shiftpv.io/v1alpha1" && string(ref.UID) == uid {
			return true
		}
	}
	return false
}
