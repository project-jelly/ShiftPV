package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"testing"

	mobilitycontroller "github.com/project-jelly/ShiftPV/src/mobility/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type retiredMoveStep func(context.Context, string, string) (bool, error)

func (f retiredMoveStep) AdvanceRetiredMove(ctx context.Context, name, uid string) (bool, error) {
	return f(ctx, name, uid)
}

func retiredMoveArgs() []string {
	return []string{"--move-name=move", "--move-uid=move-uid", "--namespace=system", "--helper-image=candidate@sha256:abc", "--helper-service-account=controller"}
}

func stoppedWriterClient() *fake.Clientset {
	zero := int32(0)
	return fake.NewSimpleClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "shiftpv-controller", Namespace: "system"}, Spec: appsv1.DeploymentSpec{
		Replicas: &zero, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"component": "controller"}},
	}})
}

func TestRetiredMoveCommandValidatesBeforeLoading(t *testing.T) {
	for _, extra := range [][]string{{"--move-name="}, {"--move-uid=.."}, {"--namespace=Bad"}, {"--node-daemonset-name=bad/name"}, {"--controller-deployment-name="}, {"--helper-image= "}, {"--helper-service-account="}, {"--timeout=0s"}, {"unexpected"}} {
		loaded := false
		err := runSettleRetiredMove(context.Background(), append(retiredMoveArgs(), extra...), io.Discard, func(retiredMoveOptions) (retiredMoveRuntime, error) {
			loaded = true
			return retiredMoveRuntime{}, nil
		})
		if err == nil || loaded {
			t.Fatal(extra, err, loaded)
		}
	}
	if err := runSettleRetiredMove(context.Background(), []string{"--help"}, io.Discard, nil); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runSettleRetiredMove(ctx, retiredMoveArgs(), io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := runSettleRetiredMove(context.Background(), retiredMoveArgs(), io.Discard, func(retiredMoveOptions) (retiredMoveRuntime, error) {
		return retiredMoveRuntime{}, io.ErrUnexpectedEOF
	}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := loadRetiredMoveRuntime(retiredMoveOptions{}); err == nil {
		t.Fatal("missing cluster configuration accepted")
	}
}

func TestRetiredMoveCommandRequiresObservedWriterExclusion(t *testing.T) {
	for _, scenario := range []string{"replicas", "remaining pod", "terminating pod", "empty selector", "unknown deployment", "unknown pods"} {
		t.Run(scenario, func(t *testing.T) {
			client := stoppedWriterClient()
			deployment, _ := client.AppsV1().Deployments("system").Get(context.Background(), "shiftpv-controller", metav1.GetOptions{})
			switch scenario {
			case "replicas":
				*deployment.Spec.Replicas = 1
			case "empty selector":
				deployment.Spec.Selector = &metav1.LabelSelector{}
			case "remaining pod", "terminating pod":
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "system", Labels: map[string]string{"component": "controller"}}}
				if scenario == "terminating pod" {
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				}
				if _, err := client.CoreV1().Pods("system").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "unknown deployment", "unknown pods":
				verb, resource := "get", "deployments"
				if scenario == "unknown pods" {
					verb, resource = "list", "pods"
				}
				client.PrependReactor(verb, resource, func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, io.ErrUnexpectedEOF })
			}
			if _, err := client.AppsV1().Deployments("system").Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			calls := 0
			err := runSettleRetiredMove(context.Background(), retiredMoveArgs(), io.Discard, func(retiredMoveOptions) (retiredMoveRuntime, error) {
				return retiredMoveRuntime{client, retiredMoveStep(func(context.Context, string, string) (bool, error) { calls++; return true, nil })}, nil
			})
			if err == nil || calls != 0 {
				t.Fatal(err, calls)
			}
		})
	}
}

func TestRetiredMoveCommandRetriesUncertaintyAndRechecksWriter(t *testing.T) {
	for _, scenario := range []string{"replay", "writer restarted", "timeout", "rejected"} {
		t.Run(scenario, func(t *testing.T) {
			client, calls := stoppedWriterClient(), 0
			args := retiredMoveArgs()
			if scenario == "timeout" {
				args = append(args, "--timeout=20ms")
			}
			var output bytes.Buffer
			err := runSettleRetiredMove(context.Background(), args, &output, func(opts retiredMoveOptions) (retiredMoveRuntime, error) {
				if opts.namespace != "system" || opts.daemonSet != "shiftpv-node" || opts.serviceAccount != "controller" || opts.helperImage != "candidate@sha256:abc" {
					t.Fatal(opts)
				}
				return retiredMoveRuntime{client, retiredMoveStep(func(ctx context.Context, name, uid string) (bool, error) {
					calls++
					if name != "move" || uid != "move-uid" {
						t.Fatal(name, uid)
					}
					if scenario == "rejected" {
						return false, mobilitycontroller.ErrRetiredMoveRejected
					}
					if scenario == "writer restarted" {
						deployment, _ := client.AppsV1().Deployments("system").Get(ctx, "shiftpv-controller", metav1.GetOptions{})
						*deployment.Spec.Replicas = 1
						if _, err := client.AppsV1().Deployments("system").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
							t.Fatal(err)
						}
					}
					if calls == 1 || scenario == "timeout" {
						return false, io.ErrUnexpectedEOF
					}
					return true, nil
				})}, nil
			})
			if scenario == "replay" {
				var report map[string]any
				if err != nil || calls != 2 || json.Unmarshal(output.Bytes(), &report) != nil || report["complete"] != true || report["moveUID"] != "move-uid" {
					t.Fatal(err, calls, output.String())
				}
			} else {
				if err == nil || calls != 1 || output.Len() != 0 {
					t.Fatal(err, calls, output.String())
				}
				if scenario == "timeout" && (!errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, io.ErrUnexpectedEOF)) {
					t.Fatal("deadline lost the pending effect diagnostic", err)
				}
			}
		})
	}
}
