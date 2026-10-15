package volumeapi

import (
	"context"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/volume"
)

const MoveProtocolPreparedV1 = "prepared-v1"

type MoveSpec struct {
	ProtocolVersion string
	VolumeID        string
	SourceNode      string
	Recovery        string
}

type MovePreparation struct {
	Pool          PoolReference `json:"pool"`
	NodeName      string        `json:"nodeName"`
	ReservedBytes int64         `json:"reservedBytes"`
}

type MoveStatus struct {
	Preparation                *MovePreparation
	PersistentVolumeUID        string
	ClaimUID                   string
	PlacementPodUID            string
	Phase                      string
	Reason                     string
	Message                    string
	LastTransitionTime         string
	LastProgressTime           string
	PersistentVolumeName       string
	ClaimNamespace             string
	ClaimName                  string
	ConsumerName               string
	ConsumerUID                string
	ReplacementName            string
	ReplacementUID             string
	DestinationNode            string
	DestinationPoolUID         string
	SourceBytes                int64
	CapacityApproved           bool
	CapacityReason             string
	CandidateNodes             []string
	EvictionRequested          bool
	CopyJobName                string
	PromotionJobName           string
	CleanupPhase               string
	CopyOperationID            string
	PromotionOperationID       string
	SourceCopy                 *volume.CopyIdentity
	IncomingCopy               *volume.CopyIdentity
	DestinationCopy            *volume.CopyIdentity
	RollbackRequiredGeneration int64
	RecoveryPhase              string
	RecoveryOwner              string
	RecoveryReason             string
	RecoveryMessage            string
}

type Move struct {
	Deleting        bool
	Name            string
	UID             string
	ResourceVersion string
	Finalizers      []string
	Spec            MoveSpec
	Status          MoveStatus
}

func MoveReservesDestination(move Move, state State, nodeName string) bool {
	return move.Status.CapacityApproved &&
		move.Status.DestinationNode == nodeName &&
		state.OwnerNode != nodeName &&
		state.ActiveMove == move.Name
}

// MoveCleanupSettled is the only terminal condition that releases a Move's
// temporary capacity holds. A terminal phase without embedded cleanup proof is
// deliberately treated as unresolved.
func MoveCleanupSettled(move Move) bool {
	if move.Status.Preparation != nil || (move.Spec.ProtocolVersion != "" && move.Spec.ProtocolVersion != MoveProtocolPreparedV1) {
		return false
	}
	if move.Spec.ProtocolVersion == MoveProtocolPreparedV1 {
		if move.Status.Phase == "Cancelled" {
			return !move.Status.CapacityApproved && !hasMoveDestinationIntent(move.Status)
		}
		if validatePreparedPlan(move) != nil {
			return false
		}
	}
	if move.Status.Phase == "Succeeded" {
		return move.Status.CleanupPhase == "Completed"
	}
	return move.Status.Phase == "Blocked" && move.Status.RecoveryPhase == "Recovered" &&
		!move.Status.CapacityApproved && move.Status.CapacityReason == "RecoverySettled"
}

func (r *Registry) CreateMove(ctx context.Context, generateName string, spec MoveSpec) (Move, error) {
	if err := r.validate(); err != nil {
		return Move{}, err
	}
	if spec.ProtocolVersion != "" && spec.ProtocolVersion != MoveProtocolPreparedV1 {
		return Move{}, fmt.Errorf("unsupported Move protocol %q", spec.ProtocolVersion)
	}
	encodedSpec := map[string]any{"volumeID": spec.VolumeID, "sourceNode": spec.SourceNode}
	options := metav1.CreateOptions{}
	if spec.ProtocolVersion != "" {
		encodedSpec["protocolVersion"] = spec.ProtocolVersion
		options.FieldValidation = metav1.FieldValidationStrict
	}
	object, err := r.Client.Resource(MoveResource).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVMove",
		"metadata":   map[string]any{"generateName": generateName, "finalizers": []any{MoveProtectionFinalizer}},
		"spec":       encodedSpec,
	}}, options)
	if err != nil {
		return Move{}, fmt.Errorf("create ShiftPVMove: %w", err)
	}
	stored, err := moveFrom(object)
	if err == nil && stored.Spec.ProtocolVersion != spec.ProtocolVersion {
		return Move{}, fmt.Errorf("%w: Move protocol was not stored; upgrade the Move CRD", ErrStateConflict)
	}
	return stored, err
}

func (r *Registry) DeleteMove(ctx context.Context, name, uid string) error {
	if err := r.validate(); err != nil {
		return err
	}
	if name == "" || uid == "" {
		return fmt.Errorf("ShiftPVMove name and UID are required for deletion")
	}
	precondition := types.UID(uid)
	err := r.Client.Resource(MoveResource).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &precondition},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ShiftPVMove: %w", err)
	}
	return nil
}

// AddMoveFinalizer re-asserts controller protection on a Move whose embedded
// cleanup journal is still unfinished. The journal store only accepts an exact
// protected parent, and updateObjectFinalizer refuses to protect an object that
// is already deleting, so this never resurrects a dying journal.
func (r *Registry) AddMoveFinalizer(ctx context.Context, name, uid string) error {
	return r.updateObjectFinalizer(ctx, MoveResource, name, uid, MoveProtectionFinalizer, true)
}

func (r *Registry) RemoveMoveFinalizer(ctx context.Context, name, uid string) error {
	return r.updateObjectFinalizer(ctx, MoveResource, name, uid, MoveProtectionFinalizer, false)
}

func (r *Registry) GetMove(ctx context.Context, name string) (Move, error) {
	if err := r.validate(); err != nil {
		return Move{}, err
	}
	object, err := r.Client.Resource(MoveResource).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return Move{}, fmt.Errorf("get ShiftPVMove: %w", err)
	}
	return moveFrom(object)
}

func (r *Registry) ListMoves(ctx context.Context) ([]Move, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	list, err := r.Client.Resource(MoveResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list ShiftPVMove: %w", err)
	}
	result := make([]Move, 0, len(list.Items))
	for index := range list.Items {
		move, moveErr := moveFrom(&list.Items[index])
		if moveErr != nil {
			return nil, fmt.Errorf("decode ShiftPVMove %q: %w", list.Items[index].GetName(), moveErr)
		}
		result = append(result, move)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result, nil
}

func (r *Registry) SetMoveStatus(ctx context.Context, name, uid string, status MoveStatus) error {
	if err := r.validate(); err != nil {
		return err
	}
	if name == "" || uid == "" {
		return fmt.Errorf("ShiftPVMove name and UID are required for status update")
	}
	return r.mutateObject(ctx, objectMutation{
		resource:   MoveResource,
		kind:       "ShiftPVMove",
		name:       name,
		uid:        uid,
		backoff:    retry.DefaultBackoff,
		readError:  "get ShiftPVMove for status update",
		writeError: "update ShiftPVMove status",
		status:     true,
		apply: func(object *unstructured.Unstructured) error {
			protocol, _, err := unstructured.NestedString(object.Object, "spec", "protocolVersion")
			if err != nil || protocol != "" {
				return fmt.Errorf("%w: versioned Move requires CompareAndSetMoveStatus", ErrStateConflict)
			}
			current, err := moveStatusFrom(object)
			if err != nil {
				return err
			}
			if current.Preparation != nil || status.Preparation != nil {
				return fmt.Errorf("%w: prepared Move requires CompareAndSetMoveStatus", ErrStateConflict)
			}
			if err := preserveMoveIdentity(current, status); err != nil {
				return err
			}
			setMoveStatus(object, status)
			return nil
		},
	})
}

func moveFrom(object *unstructured.Unstructured) (Move, error) {
	volumeID, _, _ := unstructured.NestedString(object.Object, "spec", "volumeID")
	sourceNode, _, _ := unstructured.NestedString(object.Object, "spec", "sourceNode")
	recovery, _, _ := unstructured.NestedString(object.Object, "spec", "recovery")
	protocol, _, err := unstructured.NestedString(object.Object, "spec", "protocolVersion")
	if err != nil {
		return Move{}, fmt.Errorf("decode protocolVersion: %w", err)
	}
	status, err := moveStatusFrom(object)
	if err != nil {
		return Move{}, err
	}
	return Move{
		Name: object.GetName(), UID: string(object.GetUID()), ResourceVersion: object.GetResourceVersion(), Deleting: object.GetDeletionTimestamp() != nil,
		Finalizers: append([]string(nil), object.GetFinalizers()...),
		Spec:       MoveSpec{ProtocolVersion: protocol, VolumeID: volumeID, SourceNode: sourceNode, Recovery: recovery}, Status: status,
	}, nil
}

func moveStatusFrom(object *unstructured.Unstructured) (MoveStatus, error) {
	read := func(name string) string {
		value, _, _ := unstructured.NestedString(object.Object, "status", name)
		return value
	}
	candidates, _, err := unstructured.NestedStringSlice(object.Object, "status", "candidateNodes")
	if err != nil {
		return MoveStatus{}, fmt.Errorf("decode candidateNodes: %w", err)
	}
	evictionRequested, _, _ := unstructured.NestedBool(object.Object, "status", "evictionRequested")
	rollbackGeneration, _, err := unstructured.NestedInt64(object.Object, "status", "rollbackRequiredGeneration")
	if err != nil {
		return MoveStatus{}, fmt.Errorf("decode rollbackRequiredGeneration: %w", err)
	}
	if rollbackGeneration < 0 {
		return MoveStatus{}, fmt.Errorf("rollbackRequiredGeneration must not be negative")
	}
	sourceBytes, _, _ := unstructured.NestedInt64(object.Object, "status", "sourceBytes")
	capacityApproved, _, _ := unstructured.NestedBool(object.Object, "status", "capacityApproved")
	cleanupPhase, _, _ := unstructured.NestedString(object.Object, "status", "cleanup", "status", "phase")
	readCopy := func(name string) (*volume.CopyIdentity, error) {
		data, found, nestedErr := unstructured.NestedMap(object.Object, "status", name)
		if nestedErr != nil || !found {
			return nil, nestedErr
		}
		var identity volume.CopyIdentity
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(data, &identity); err != nil {
			return nil, err
		}
		if err := identity.Validate(); err != nil {
			return nil, err
		}
		return &identity, nil
	}
	sourceCopy, err := readCopy("sourceCopy")
	if err != nil {
		return MoveStatus{}, fmt.Errorf("decode sourceCopy: %w", err)
	}
	incomingCopy, err := readCopy("incomingCopy")
	if err != nil {
		return MoveStatus{}, fmt.Errorf("decode incomingCopy: %w", err)
	}
	destinationCopy, err := readCopy("destinationCopy")
	if err != nil {
		return MoveStatus{}, fmt.Errorf("decode destinationCopy: %w", err)
	}
	preparation, err := movePreparationFrom(object)
	if err != nil {
		return MoveStatus{}, err
	}
	return MoveStatus{
		Preparation: preparation, PersistentVolumeUID: read("persistentVolumeUID"), ClaimUID: read("persistentVolumeClaimUID"), PlacementPodUID: read("placementPodUID"),
		Phase: read("phase"), Reason: read("reason"), Message: read("message"),
		LastTransitionTime: read("lastTransitionTime"), LastProgressTime: read("lastProgressTime"),
		PersistentVolumeName: read("persistentVolumeName"), ClaimNamespace: read("persistentVolumeClaimNamespace"),
		ClaimName: read("persistentVolumeClaimName"), ConsumerName: read("consumerName"), ConsumerUID: read("consumerUID"), ReplacementName: read("replacementName"),
		ReplacementUID:  read("replacementUID"),
		DestinationNode: read("destinationNode"), DestinationPoolUID: read("destinationPoolUID"), SourceBytes: sourceBytes, CapacityApproved: capacityApproved,
		CapacityReason: read("capacityReason"), CandidateNodes: candidates, EvictionRequested: evictionRequested,
		CopyJobName: read("copyJobName"), PromotionJobName: read("promotionJobName"), CleanupPhase: cleanupPhase,
		CopyOperationID: read("copyOperationID"), PromotionOperationID: read("promotionOperationID"),
		SourceCopy: sourceCopy, IncomingCopy: incomingCopy, DestinationCopy: destinationCopy,
		RollbackRequiredGeneration: rollbackGeneration,
		RecoveryPhase:              read("recoveryPhase"), RecoveryOwner: read("recoveryOwner"),
		RecoveryReason: read("recoveryReason"), RecoveryMessage: read("recoveryMessage"),
	}, nil
}

func setMoveStatus(object *unstructured.Unstructured, status MoveStatus) {
	previous, _ := object.Object["status"].(map[string]any)
	cleanup := previous["cleanup"]
	next := map[string]any{
		"phase": status.Phase, "reason": status.Reason, "message": status.Message,
		"lastTransitionTime": status.LastTransitionTime, "lastProgressTime": status.LastProgressTime,
		"persistentVolumeName": status.PersistentVolumeName, "persistentVolumeClaimNamespace": status.ClaimNamespace,
		"persistentVolumeClaimName": status.ClaimName, "consumerName": status.ConsumerName, "consumerUID": status.ConsumerUID, "replacementName": status.ReplacementName,
		"replacementUID":  status.ReplacementUID,
		"destinationNode": status.DestinationNode, "destinationPoolUID": status.DestinationPoolUID, "sourceBytes": status.SourceBytes,
		"capacityApproved": status.CapacityApproved, "capacityReason": status.CapacityReason,
		"candidateNodes":    stringSliceToAny(status.CandidateNodes),
		"evictionRequested": status.EvictionRequested, "copyJobName": status.CopyJobName,
		"promotionJobName": status.PromotionJobName,
		"copyOperationID":  status.CopyOperationID, "promotionOperationID": status.PromotionOperationID,
		"recoveryPhase": status.RecoveryPhase, "recoveryOwner": status.RecoveryOwner,
		"recoveryReason": status.RecoveryReason, "recoveryMessage": status.RecoveryMessage,
	}
	for key, value := range map[string]string{"persistentVolumeUID": status.PersistentVolumeUID, "persistentVolumeClaimUID": status.ClaimUID, "placementPodUID": status.PlacementPodUID} {
		if value != "" {
			next[key] = value
		}
	}
	if status.Preparation != nil {
		next["preparation"] = map[string]any{"pool": map[string]any{"name": status.Preparation.Pool.Name, "uid": status.Preparation.Pool.UID}, "nodeName": status.Preparation.NodeName, "reservedBytes": status.Preparation.ReservedBytes}
	}
	if status.RollbackRequiredGeneration > 0 {
		next["rollbackRequiredGeneration"] = status.RollbackRequiredGeneration
	}
	object.Object["status"] = next
	encoded := next
	if cleanup != nil {
		encoded["cleanup"] = cleanup
	}
	for name, identity := range map[string]*volume.CopyIdentity{"sourceCopy": status.SourceCopy, "incomingCopy": status.IncomingCopy, "destinationCopy": status.DestinationCopy} {
		if identity == nil {
			continue
		}
		if value, err := runtime.DefaultUnstructuredConverter.ToUnstructured(identity); err == nil {
			encoded[name] = value
		}
	}
}

func preserveMoveIdentity(current, next MoveStatus) error {
	if next.SourceBytes < 0 || (current.SourceBytes > 0 && current.SourceBytes != next.SourceBytes) {
		return fmt.Errorf("%w: recorded Move sourceBytes is immutable", ErrStateConflict)
	}
	if next.RollbackRequiredGeneration < 0 || (current.RollbackRequiredGeneration > 0 && current.RollbackRequiredGeneration != next.RollbackRequiredGeneration) {
		return fmt.Errorf("%w: Move rollbackRequiredGeneration is immutable", ErrStateConflict)
	}

	for _, item := range []struct {
		name          string
		current, next *volume.CopyIdentity
	}{
		{"sourceCopy", current.SourceCopy, next.SourceCopy},
		{"incomingCopy", current.IncomingCopy, next.IncomingCopy},
		{"destinationCopy", current.DestinationCopy, next.DestinationCopy},
	} {
		if item.current != nil && (item.next == nil || *item.current != *item.next) {
			return fmt.Errorf("%w: Move %s is immutable", ErrStateConflict, item.name)
		}
	}
	for _, item := range []struct{ name, current, next string }{
		{"copyOperationID", current.CopyOperationID, next.CopyOperationID},
		{"promotionOperationID", current.PromotionOperationID, next.PromotionOperationID},
	} {
		if item.current != "" && item.current != item.next {
			return fmt.Errorf("%w: Move %s is immutable", ErrStateConflict, item.name)
		}
	}
	return nil
}
