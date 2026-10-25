// Package consumer determines whether capacity denial may release a PVC's node.
package consumer

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
)

const (
	SelectedNode  = "volume.kubernetes.io/selected-node"
	PopulatorKind = "cdi.kubevirt.io/storage.populator.kind"
	PrimeName     = "cdi.kubevirt.io/storage.populator.pvcPrime"
)

var ErrIdentity = errors.New("PVC placement identity changed")

type Placement uint8

const (
	Unknown Placement = iota
	Fixed
	Reschedulable
)

type Request struct{ Namespace, Name, UID, Node string }
type Result struct {
	Placement Placement
	Reason    string
}

// Inspector is injected into CSI admission. Errors must never authorize rescheduling.
type Inspection interface {
	Inspect(context.Context, Request) (Result, error)
}

// Reader exposes only the observations needed to decide placement ownership.
type Reader interface {
	Claim(context.Context, string, string) (*corev1.PersistentVolumeClaim, error)
	Pod(context.Context, string, string) (*corev1.Pod, error)
	Pods(context.Context, string) ([]corev1.Pod, error)
}
