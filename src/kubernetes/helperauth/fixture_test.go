package helperauth

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func setVolumeStateFixture(t *testing.T, client dynamic.Interface, volumeID string, state volumeapi.State) {
	t.Helper()
	resource := client.Resource(volumeapi.VolumeResource)
	object, err := resource.Get(context.Background(), volumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ShiftPVVolume fixture: %v", err)
	}
	previous, _ := object.Object["status"].(map[string]any)
	status := map[string]any{
		"phase": state.Phase, "ownerNode": state.OwnerNode, "activeMove": state.ActiveMove,
		"publishedNodes": stringSliceToAnyFixture(state.PublishedNodes),
	}
	if cleanup := previous["cleanup"]; cleanup != nil {
		status["cleanup"] = cleanup
	}
	for name, value := range map[string]string{
		"creationOperationID": state.CreationOperationID,
		"deletionOperationID": state.DeletionOperationID,
	} {
		if value != "" {
			status[name] = value
		}
	}
	if state.CurrentCopy != nil {
		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(state.CurrentCopy)
		if err != nil {
			t.Fatalf("encode current copy fixture: %v", err)
		}
		status["currentCopy"] = encoded
	}
	object.Object["status"] = status
	if _, err := resource.UpdateStatus(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update ShiftPVVolume fixture: %v", err)
	}
}

func stringSliceToAnyFixture(values []string) []any {
	result := make([]any, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}

// Seed authority inputs independently of any executable Move status writer.
func setMoveStatusFixture(t *testing.T, client dynamic.Interface, name string, state volumeapi.MoveStatus) {
	t.Helper()
	resource := client.Resource(volumeapi.MoveResource)
	object, err := resource.Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]any{
		"phase": state.Phase, "recoveryPhase": state.RecoveryPhase, "recoveryOwner": state.RecoveryOwner,
		"destinationNode": state.DestinationNode, "destinationPoolUID": state.DestinationPoolUID,
		"copyOperationID": state.CopyOperationID, "copyJobName": state.CopyJobName,
		"promotionOperationID": state.PromotionOperationID, "promotionJobName": state.PromotionJobName,
	}
	for field, value := range map[string]string{
		"persistentVolumeName": state.PersistentVolumeName, "persistentVolumeUID": state.PersistentVolumeUID,
		"persistentVolumeClaimNamespace": state.ClaimNamespace, "persistentVolumeClaimName": state.ClaimName,
		"persistentVolumeClaimUID": state.ClaimUID, "consumerName": state.ConsumerName, "consumerUID": state.ConsumerUID,
	} {
		if value != "" {
			status[field] = value
		}
	}
	if state.SettlementRequested {
		status["settlementRequested"] = true
	}
	if state.RollbackRequiredGeneration > 0 {
		status["rollbackRequiredGeneration"] = state.RollbackRequiredGeneration
	}
	for field, copy := range map[string]*volume.CopyIdentity{"sourceCopy": state.SourceCopy, "incomingCopy": state.IncomingCopy, "destinationCopy": state.DestinationCopy} {
		if copy == nil {
			continue
		}
		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(copy)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > 0 {
			status[field] = encoded
		}
	}
	object.Object["status"] = status
	if _, err := resource.UpdateStatus(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
