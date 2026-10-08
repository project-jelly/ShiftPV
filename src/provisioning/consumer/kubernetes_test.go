package consumer

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
)

func TestKubernetesReaderScopesConsumerObservation(t *testing.T) {
	r, req := fixture()
	foreign := consumerPod("other")
	foreign.Namespace = "foreign"
	client := fake.NewClientset(r.claims[req.Name], &foreign)
	inspection := Inspector{Reader: KubernetesReader{Client: client}}
	result, err := inspection.Inspect(context.Background(), req)
	if err != nil || result.Placement != Reschedulable {
		t.Fatalf("another namespace affected placement: %+v %v", result, err)
	}
	local := consumerPod("node")
	local.Namespace = req.Namespace
	if _, err := client.CoreV1().Pods(req.Namespace).Create(context.Background(), &local, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err = inspection.Inspect(context.Background(), req)
	if err != nil || result.Placement != Fixed {
		t.Fatalf("local consumer not observed: %+v %v", result, err)
	}
	reader := KubernetesReader{Client: client}
	pod, err := reader.Pod(context.Background(), req.Namespace, local.Name)
	if err != nil || pod.UID != local.UID {
		t.Fatalf("Pod identity lost: %v", err)
	}
	outage := errors.New("list unavailable")
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, outage })
	result, err = inspection.Inspect(context.Background(), req)
	if !errors.Is(err, outage) || result.Placement == Reschedulable {
		t.Fatalf("adapter hid observation failure: %+v %v", result, err)
	}
}
