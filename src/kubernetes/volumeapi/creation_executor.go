package volumeapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// NodeExecutor pins execution to one incarnation of the resident Node Pod.
type NodeExecutor struct {
	Namespace string `json:"namespace"`
	PodName   string `json:"podName"`
	PodUID    string `json:"podUID"`
	NodeName  string `json:"nodeName"`
}

func (e NodeExecutor) Valid() bool {
	return volume.ValidObjectName(e.Namespace) && volume.ValidObjectName(e.PodName) && volume.ValidIdentityToken(e.PodUID) && volume.ValidObjectName(e.NodeName)
}

type CreationReceipt struct {
	OperationID        string `json:"operationID"`
	ExecutorUID        string `json:"executorUID"`
	ObservedAt         string `json:"observedAt"`
	LocalReceiptDigest string `json:"localReceiptDigest"`
}

func ValidCreationReceipt(s State) bool {
	if s.CreationExecutor == nil || !s.CreationExecutor.Valid() || s.CreationReceipt == nil || s.CurrentCopy == nil || s.CurrentCopy.Validate() != nil || s.CurrentCopy.VolumeUID != s.UID || s.CurrentCopy.Role != volume.RoleServing {
		return false
	}
	receipt := s.CreationReceipt
	digest, err := hex.DecodeString(receipt.LocalReceiptDigest)
	_, timeErr := time.Parse(time.RFC3339Nano, receipt.ObservedAt)
	operation, opErr := CreationOperationID(s.UID)
	return err == nil && len(digest) == sha256.Size && hex.EncodeToString(digest) == receipt.LocalReceiptDigest && timeErr == nil && opErr == nil &&
		receipt.OperationID == operation && receipt.OperationID == s.CreationOperationID && receipt.ExecutorUID == s.CreationExecutor.PodUID && s.CreationExecutor.NodeName == s.CurrentCopy.NodeName
}

// BindCreation switches an unexecuted Pending intent to NodeCreating. Older
// helpers cannot authorize this phase. Rebinding retains the exact operation
// and copy, whose local filesystem lock serializes Pod incarnations.
func (r *Registry) BindCreation(ctx context.Context, expected State, executor NodeExecutor) error {
	if !executor.Valid() || expected.CurrentCopy == nil || executor.NodeName != expected.CurrentCopy.NodeName {
		return ErrStateConflict
	}
	return r.mutateState(ctx, expected.CurrentCopy.VolumeID, func(current State) (State, error) {
		if !sameNodeCreation(current, expected) || current.CreationReceipt != nil || (current.Phase != PhasePending && current.Phase != PhaseNodeCreating) || !reflect.DeepEqual(current.CreationExecutor, expected.CreationExecutor) {
			return State{}, ErrStateConflict
		}
		current.Phase = PhaseNodeCreating
		current.CreationExecutor = &executor
		return current, nil
	})
}
func sameNodeCreation(current, expected State) bool {
	return current.UID != "" && current.UID == expected.UID && current.CreationOperationID == expected.CreationOperationID &&
		current.CurrentCopy != nil && expected.CurrentCopy != nil && *current.CurrentCopy == *expected.CurrentCopy && current.OwnerNode == expected.OwnerNode && current.CurrentCopy.VolumeUID == current.UID && current.CurrentCopy.NodeName == current.OwnerNode && current.CreationOperationID == "create-"+current.UID &&
		current.ActiveMove == "" && len(current.PublishedNodes) == 0 && slices.Contains(current.Finalizers, VolumeProtectionFinalizer)
}

// RecordCreationReceipt accepts evidence only from the currently bound Pod.
func (r *Registry) RecordCreationReceipt(ctx context.Context, expected State, receipt CreationReceipt) error {
	if expected.CurrentCopy == nil {
		return ErrStateConflict
	}
	return r.mutateState(ctx, expected.CurrentCopy.VolumeID, func(current State) (State, error) {
		if !sameNodeCreation(current, expected) || current.Phase != PhaseNodeCreating || !reflect.DeepEqual(current.CreationExecutor, expected.CreationExecutor) {
			return State{}, ErrStateConflict
		}
		if current.CreationReceipt != nil {
			if *current.CreationReceipt != receipt {
				return State{}, ErrStateConflict
			}
			return current, nil
		}
		current.CreationReceipt = &receipt
		if !ValidCreationReceipt(current) {
			return State{}, ErrStateConflict
		}
		return current, nil
	})
}

func decodeStatusField(object *unstructured.Unstructured, name string, target any) error {
	value, found, err := unstructured.NestedMap(object.Object, "status", name)
	if err != nil || !found {
		return err
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(value, target); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}
func setStatusField(object *unstructured.Unstructured, name string, value any) {
	if reflect.ValueOf(value).IsNil() {
		return
	}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(value)
	if err == nil {
		_ = unstructured.SetNestedMap(object.Object, encoded, "status", name)
	}
}
