package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
)

func TestCancelledCreationRPCDoesNotReplayHistoricalCreationReceipt(t *testing.T) {
	client, node, _, _, initial := fixture(t)
	ctx := context.Background()
	if err := client.Volumes.BindCreation(ctx, initial, node.Identity); err != nil {
		t.Fatal(err)
	}
	current, err := client.Volumes.Get(ctx, testID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.RecordCreationReceipt(ctx, current, creationReceipt(node.Identity)); err != nil {
		t.Fatal(err)
	}
	current, err = client.Volumes.BeginCreationCancellation(ctx, testID, initial.UID, *initial.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	if !volumeapi.ValidCreationReceipt(current) {
		t.Fatal("fixture lost historical receipt")
	}
	request := &protocol.EffectRequest{OperationId: initial.CreationOperationID}
	if complete, err := node.creationRPC(current, request); complete || !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("cancelled creation replayed completion: complete=%v err=%v", complete, err)
	}
}
