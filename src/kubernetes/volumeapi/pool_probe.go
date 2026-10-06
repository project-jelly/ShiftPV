package volumeapi

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
)

const PoolCapacityProbeRequestAnnotation = "shiftpv.io/capacity-probe-request"

func (r *Registry) RequestPoolCapacityProbe(ctx context.Context, expected Pool, request string) error {
	if request == "" || len(request) > 512 {
		return fmt.Errorf("capacity probe request is missing or too large")
	}
	return r.mutatePoolProbe(ctx, expected, false, func(object *unstructured.Unstructured) error {
		annotations := object.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[PoolCapacityProbeRequestAnnotation] = request
		object.SetAnnotations(annotations)
		return nil
	})
}

func (r *Registry) RecordPoolCapacityProbe(ctx context.Context, expected Pool, result PoolCapacityProbeResult) error {
	return r.mutatePoolProbe(ctx, expected, true, func(object *unstructured.Unstructured) error {
		if expected.CapacityProbeRequest == "" || object.GetAnnotations()[PoolCapacityProbeRequestAnnotation] != expected.CapacityProbeRequest {
			return fmt.Errorf("%w: capacity probe request changed", ErrStateConflict)
		}
		data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&result)
		if err != nil {
			return err
		}
		return unstructured.SetNestedMap(object.Object, data, "status", "capacityProbe")
	})
}

func (r *Registry) mutatePoolProbe(ctx context.Context, expected Pool, status bool, apply func(*unstructured.Unstructured) error) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.mutateObject(ctx, objectMutation{
		resource: PoolResource, kind: "ShiftPVPool", name: expected.Name, uid: expected.UID,
		backoff: retry.DefaultRetry, status: status,
		readError: "read capacity probe Pool", writeError: "update capacity probe Pool",
		apply: func(object *unstructured.Unstructured) error {
			current, err := poolFrom(object)
			if err != nil {
				return err
			}
			if current.NodeName != expected.NodeName || current.Generation != expected.Generation || current.MountPath != expected.MountPath ||
				current.DeletionTimestamp != nil || !reflect.DeepEqual(current.Status.MountIdentity, expected.Status.MountIdentity) ||
				!reflect.DeepEqual(current.Status.CapacityUnit, expected.Status.CapacityUnit) {
				return fmt.Errorf("%w: capacity probe Pool evidence changed", ErrStateConflict)
			}
			return apply(object)
		},
	})
}
