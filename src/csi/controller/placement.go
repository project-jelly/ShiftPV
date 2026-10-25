package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A missing registration can require a different node. A registered Pool whose
// observation is unavailable must first recover. Neither permits relocating a
// fixed or unproven consumer; existing creation intents are pinned by the caller.
func (s *Service) noReadyPool(ctx context.Context, parameters map[string]string, requestName, node string, registered []volumeapi.Pool) error {
	group := requestedPoolGroup(parameters)
	for _, pool := range registered {
		if pool.NodeName == node && poolGroup(pool) == group {
			s.waitForPoolObservation(parameters, requestName, node, volumeapi.Pool{})
			return status.Errorf(codes.Unavailable, "node %q has registered Pools in group %q, but none is Ready; wait for Pool observation", node, group)
		}
	}
	message := fmt.Sprintf("node %q has no registered Pool in group %q", node, group)
	placement, err := s.inspectConsumerPlacement(ctx, parameters, requestName, node, message)
	if err != nil {
		return err
	}
	if placement.Placement == consumer.Reschedulable {
		return status.Error(codes.ResourceExhausted, message)
	}
	s.waitForPoolObservation(parameters, requestName, node, volumeapi.Pool{})
	return status.Errorf(codes.Unavailable, "%s; %s; preserve node %q and retry", message, placement.Reason, node)
}

func (s *Service) inspectConsumerPlacement(ctx context.Context, parameters map[string]string, requestName, node, message string) (consumer.Result, error) {
	if s.ConsumerPlacement == nil {
		return consumer.Result{}, status.Error(codes.Unavailable, "consumer placement inspection is not configured")
	}
	placement, err := s.ConsumerPlacement.Inspect(ctx, consumer.Request{Namespace: parameters[PVCNamespaceKey], Name: parameters[PVCNameKey], UID: strings.TrimPrefix(requestName, "pvc-"), Node: node})
	if errors.Is(err, consumer.ErrIdentity) {
		return consumer.Result{}, status.Errorf(codes.FailedPrecondition, "%s; %v", message, err)
	}
	if err != nil {
		return consumer.Result{}, status.Errorf(codes.Unavailable, "%s; inspect PVC consumer: %v", message, err)
	}
	return placement, nil
}
