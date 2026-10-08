package consumer

import (
	"context"
	"errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"
)

type memoryReader struct {
	claims                          map[string]*corev1.PersistentVolumeClaim
	pods                            []corev1.Pod
	claimError, podError, listError error
	claimErrors                     map[string]error
	calls                           []string
}

func (r *memoryReader) Claim(_ context.Context, ns, name string) (*corev1.PersistentVolumeClaim, error) {
	r.calls = append(r.calls, "claim:"+name)
	if err := r.claimErrors[name]; err != nil {
		return nil, err
	}
	if r.claimError != nil {
		return nil, r.claimError
	}
	if p := r.claims[name]; p != nil {
		return p.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, name)
}
func (r *memoryReader) Pod(_ context.Context, ns, name string) (*corev1.Pod, error) {
	r.calls = append(r.calls, "pod:"+name)
	if r.podError != nil {
		return nil, r.podError
	}
	for _, p := range r.pods {
		if p.Name == name {
			return p.DeepCopy(), nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
}
func (r *memoryReader) Pods(_ context.Context, ns string) ([]corev1.Pod, error) {
	r.calls = append(r.calls, "pods")
	return r.pods, r.listError
}
func fixture() (*memoryReader, Request) {
	req := Request{Namespace: "test", Name: "claim", UID: "claim-uid", Node: "node"}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: req.Name, Namespace: req.Namespace, UID: "claim-uid", Annotations: map[string]string{SelectedNode: req.Node}}}
	return &memoryReader{claims: map[string]*corev1.PersistentVolumeClaim{req.Name: pvc}}, req
}
func consumerPod(node string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", UID: "pod-uid"}, Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim"}}}}}}
}
func primeFixture(kind string) (*memoryReader, Request) {
	r, req := fixture()
	pvc := r.claims[req.Name]
	pvc.Annotations[PopulatorKind] = kind
	control := true
	pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "target", UID: "target-uid", Controller: &control}}
	group := "cdi.kubevirt.io"
	r.claims["target"] = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "target", UID: "target-uid", Annotations: map[string]string{SelectedNode: req.Node}}, Spec: corev1.PersistentVolumeClaimSpec{DataSourceRef: &corev1.TypedObjectReference{APIGroup: &group, Kind: kind, Name: "source"}}}
	return r, req
}
func TestConsumerPlacementPatterns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*memoryReader, *Request)
		want     Placement
		identity bool
	}{
		{name: "ordinary PVC before scheduling", want: Reschedulable},
		{name: "ordinary unscheduled consumer", change: func(r *memoryReader, _ *Request) { r.pods = []corev1.Pod{consumerPod("")} }, want: Reschedulable},
		{name: "scheduled consumer without Pod ownership", change: func(r *memoryReader, _ *Request) { r.pods = []corev1.Pod{consumerPod("node")} }, want: Fixed},
		{name: "consumer on another node", change: func(r *memoryReader, _ *Request) { r.pods = []corev1.Pod{consumerPod("other")} }, want: Unknown},
		{name: "terminal consumer", change: func(r *memoryReader, _ *Request) {
			pod := consumerPod("node")
			pod.Status.Phase = corev1.PodSucceeded
			r.pods = []corev1.Pod{pod}
		}, want: Reschedulable},
		{name: "unrelated scheduled Pod", change: func(r *memoryReader, _ *Request) {
			pod := consumerPod("node")
			pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other"
			r.pods = []corev1.Pod{pod}
		}, want: Reschedulable},
		{name: "missing owner Pod", change: func(r *memoryReader, q *Request) {
			r.claims[q.Name].OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "missing", UID: "missing"}}
		}, want: Unknown},
		{name: "owner pending assignment", change: func(r *memoryReader, q *Request) {
			r.pods = []corev1.Pod{consumerPod("")}
			r.claims[q.Name].OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "worker", UID: "pod-uid"}}
		}, want: Unknown},
		{name: "metadata missing", change: func(_ *memoryReader, q *Request) { q.Name = "" }, want: Unknown},
		{name: "PVC absent", change: func(r *memoryReader, q *Request) { delete(r.claims, q.Name) }, want: Unknown},
		{name: "PVC name reused", change: func(r *memoryReader, q *Request) { r.claims[q.Name].UID = "replacement" }, identity: true},
		{name: "node changed", change: func(r *memoryReader, q *Request) { r.claims[q.Name].Annotations[SelectedNode] = "other" }, identity: true},
		{name: "node lost", change: func(r *memoryReader, q *Request) { delete(r.claims[q.Name].Annotations, SelectedNode) }, identity: true},
		{name: "PVC deletion started", change: func(r *memoryReader, q *Request) { now := metav1.Now(); r.claims[q.Name].DeletionTimestamp = &now }, identity: true},
		{name: "PVC already bound", change: func(r *memoryReader, q *Request) { r.claims[q.Name].Spec.VolumeName = "pv" }, identity: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, q := fixture()
			if tc.change != nil {
				tc.change(r, &q)
			}
			result, err := (Inspector{Reader: r}).Inspect(context.Background(), q)
			if tc.identity {
				if !errors.Is(err, ErrIdentity) {
					t.Fatalf("want identity rejection, got %v", err)
				}
				return
			}
			if err != nil || result.Placement != tc.want {
				t.Fatalf("result=%+v err=%v want=%v", result, err, tc.want)
			}
		})
	}
}
func TestPrimeIsFixedBeforePodCreation(t *testing.T) {
	for _, kind := range []string{"VolumeImportSource", "VolumeUploadSource"} {
		t.Run(kind, func(t *testing.T) {
			r, q := primeFixture(kind)
			result, err := (Inspector{Reader: r}).Inspect(context.Background(), q)
			if err != nil || result.Placement != Fixed {
				t.Fatalf("%+v %v", result, err)
			}
			if len(r.calls) != 2 || r.calls[0] != "claim:claim" || r.calls[1] != "claim:target" {
				t.Fatalf("prime classification must precede Pod creation: %v", r.calls)
			}
		})
	}
}
func TestPrimeRequiresExactParentContract(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*memoryReader, Request)
		identity bool
	}{
		{"parent UID changed", func(r *memoryReader, _ Request) { r.claims["target"].UID = "replacement" }, true},
		{"parent node changed", func(r *memoryReader, _ Request) { r.claims["target"].Annotations[SelectedNode] = "other" }, true},
		{"source kind changed", func(r *memoryReader, _ Request) { r.claims["target"].Spec.DataSourceRef.Kind = "VolumeUploadSource" }, true},
		{"prime annotation names another claim", func(r *memoryReader, _ Request) { r.claims["target"].Annotations[PrimeName] = "another" }, true},
		{"missing parent", func(r *memoryReader, _ Request) { delete(r.claims, "target") }, false},
		{"missing controller owner", func(r *memoryReader, q Request) { r.claims[q.Name].OwnerReferences = nil }, false},
		{"unknown populator", func(r *memoryReader, q Request) { r.claims[q.Name].Annotations[PopulatorKind] = "Other" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, q := primeFixture("VolumeImportSource")
			tc.change(r, q)
			result, err := (Inspector{Reader: r}).Inspect(context.Background(), q)
			if tc.identity {
				if !errors.Is(err, ErrIdentity) {
					t.Fatalf("expected identity error: %v", err)
				}
			} else if err != nil || result.Placement != Unknown {
				t.Fatalf("expected unknown: %+v %v", result, err)
			}
		})
	}
}
func TestObservationErrorsCannotAuthorizeRescheduling(t *testing.T) {
	outage := errors.New("injected read failure")
	for _, observation := range []string{"claim", "pods", "owner Pod", "parent"} {
		t.Run(observation, func(t *testing.T) {
			r, q := fixture()
			switch observation {
			case "claim":
				r.claimError = outage
			case "pods":
				r.listError = outage
			case "owner Pod":
				r.claims[q.Name].OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "worker"}}
				r.podError = outage
			case "parent":
				r, q = primeFixture("VolumeImportSource")
				r.claimErrors = map[string]error{"target": outage}
			}
			result, err := (Inspector{Reader: r}).Inspect(context.Background(), q)
			if !errors.Is(err, outage) || result.Placement == Reschedulable {
				t.Fatalf("failure authorized rescheduling: %+v %v", result, err)
			}
		})
	}
}

func TestConflictingConsumersCannotAuthorizeRescheduling(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		a, b := consumerPod("node"), consumerPod("other")
		pods := []corev1.Pod{a, b}
		if reverse {
			pods = []corev1.Pod{b, a}
		}
		if got := placementForPods(pods, "claim", "node"); got.Placement != Unknown {
			t.Fatalf("ordering hid conflict: %+v", got)
		}
	}
}
func TestOwnerIdentityRemainsUncertain(t *testing.T) {
	for _, unrelated := range []bool{false, true} {
		r, q := fixture()
		pod := consumerPod("")
		uid := pod.UID
		if unrelated {
			pod.Spec.Volumes = nil
		} else {
			uid = "replaced"
		}
		r.pods = []corev1.Pod{pod}
		r.claims[q.Name].OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: uid}}
		result, err := (Inspector{Reader: r}).Inspect(context.Background(), q)
		if err != nil || result.Placement != Unknown {
			t.Fatalf("unproven owner allowed scheduling: %+v %v", result, err)
		}
	}
}
func TestInspectorRequiresReader(t *testing.T) {
	_, q := fixture()
	result, err := (Inspector{}).Inspect(context.Background(), q)
	if err == nil || result.Placement == Reschedulable {
		t.Fatalf("missing dependency authorized scheduling: %+v %v", result, err)
	}
}

func TestGenericEphemeralOwnerPreservesSchedulerContract(t *testing.T) {
	for _, node := range []string{"", "node"} {
		r, q := fixture()
		pod := consumerPod(node)
		pod.Namespace = q.Namespace
		pod.Spec.Volumes = []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}}}
		old := q.Name
		q.Name = pod.Name + "-scratch"
		pvc := r.claims[old]
		delete(r.claims, old)
		pvc.Name = q.Name
		control := true
		pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID, Controller: &control}}
		r.claims[q.Name] = pvc
		r.pods = []corev1.Pod{pod}
		want := Reschedulable
		if node != "" {
			want = Fixed
		}
		got, err := (Inspector{Reader: r}).Inspect(context.Background(), q)
		if err != nil || got.Placement != want {
			t.Fatalf("node=%q got=%+v err=%v want=%v", node, got, err, want)
		}
	}
}
