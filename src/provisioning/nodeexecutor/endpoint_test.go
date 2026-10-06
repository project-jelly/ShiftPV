package nodeexecutor

import (
	"context"
	"errors"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEndpointRevalidatesBoundPodOwnerAndRegistration(t *testing.T) {
	for _, test := range []struct {
		name        string
		change      func(*corev1.Pod)
		wantSupport bool
		wantError   bool
	}{
		{name: "old transport"},
		{name: "IPv6", wantSupport: true, change: func(p *corev1.Pod) {
			p.Annotations[security.CertificateAnnotation] = "public"
			p.Annotations[security.PortAnnotation] = "9760"
			p.Status.PodIP = "2001:db8::1"
		}},
		{name: "replacement Pod", wantError: true, change: func(p *corev1.Pod) { p.UID = "replacement" }},
		{name: "foreign owner", wantError: true, change: func(p *corev1.Pod) { p.OwnerReferences[0].UID = "replacement-ds" }},
		{name: "wrong ServiceAccount", wantError: true, change: func(p *corev1.Pod) { p.Spec.ServiceAccountName = "foreign" }},
		{name: "port invalid", wantError: true, change: func(p *corev1.Pod) {
			p.Annotations[security.CertificateAnnotation] = "public"
			p.Annotations[security.PortAnnotation] = "65536"
			p.Status.PodIP = "127.0.0.1"
		}},
		{name: "missing IP", wantError: true, change: func(p *corev1.Pod) {
			p.Annotations[security.CertificateAnnotation] = "public"
			p.Annotations[security.PortAnnotation] = "9760"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, node, _, kube, _ := fixture(t)
			ctx := context.Background()
			pod, err := kube.CoreV1().Pods(node.Identity.Namespace).Get(ctx, node.Identity.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if test.change != nil {
				test.change(pod)
			}
			if _, err := kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			target, supported, err := client.Discovery.Endpoint(ctx, node.Identity)
			if (err != nil) != test.wantError || supported != test.wantSupport {
				t.Fatalf("endpoint=%+v supported=%v err=%v", target, supported, err)
			}
			if test.name == "IPv6" && target.Address != "[2001:db8::1]:9760" {
				t.Fatalf("IPv6 endpoint=%s", target.Address)
			}
		})
	}
	client, node, _, _, _ := fixture(t)
	identity := node.Identity
	identity.Namespace = "foreign"
	if _, _, err := client.Discovery.Endpoint(context.Background(), identity); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatal("cross-installation endpoint accepted")
	}
}
