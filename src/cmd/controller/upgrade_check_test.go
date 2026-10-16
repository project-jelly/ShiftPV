package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/lifecycle/upgrade"
)

func TestUpgradeCheckParsesBeforeLoadingClients(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	for _, args := range [][]string{
		{}, {"--namespace=system", "--timeout=0"}, {"--namespace=system", "--node-daemonset-name="},
		{"--namespace=system", "extra"}, {"--unknown-flag"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			loaded := false
			err := runUpgradeCheck(context.Background(), args, io.Discard, func() (kubernetes.Interface, dynamic.Interface, error) {
				loaded = true
				return nil, nil, nil
			})
			if err == nil || loaded {
				t.Fatalf("invalid flags reached Kubernetes: err=%v loaded=%v", err, loaded)
			}
		})
	}
	var help bytes.Buffer
	err := runUpgradeCheck(context.Background(), []string{"--help"}, &help, nil)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "read-only") {
		t.Fatalf("help initialized the controller or lost inspection usage: %v %q", err, help.String())
	}
}

func TestUpgradeCheckReportsOnlyCompleteReadOnlyInspection(t *testing.T) {
	typed := fake.NewClientset(&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "nodes", Namespace: "system", UID: "daemon-uid"}})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
	})
	var output bytes.Buffer
	err := runUpgradeCheck(context.Background(), []string{"--namespace=system", "--node-daemonset-name=nodes"}, &output,
		func() (kubernetes.Interface, dynamic.Interface, error) { return typed, dyn, nil })
	if err != nil {
		t.Fatal(err)
	}
	var report upgrade.Report
	if err := json.Unmarshal(output.Bytes(), &report); err != nil || !report.Safe() {
		t.Fatalf("success did not carry a complete report: report=%+v err=%v", report, err)
	}
	for _, actions := range [][]ktesting.Action{typed.Actions(), dyn.Actions()} {
		for _, action := range actions {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatalf("inspection changed cluster state: %s %s", action.GetVerb(), action.GetResource().Resource)
			}
		}
	}

	dyn.PrependReactor("list", "shiftpvmoves", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("inspection unavailable")
	})
	output.Reset()
	err = runUpgradeCheck(context.Background(), []string{"--namespace=system", "--node-daemonset-name=nodes"}, &output,
		func() (kubernetes.Interface, dynamic.Interface, error) { return typed, dyn, nil })
	if err == nil || output.Len() != 0 {
		t.Fatalf("failed observation emitted a usable report: err=%v report=%q", err, output.String())
	}
}

func TestUpgradeCheckDoesNotBootstrapAfterClientFailure(t *testing.T) {
	want := errors.New("client setup failed")
	var output bytes.Buffer
	err := runUpgradeCheck(context.Background(), []string{"--namespace=system"}, &output,
		func() (kubernetes.Interface, dynamic.Interface, error) { return nil, nil, want })
	if !errors.Is(err, want) || output.Len() != 0 {
		t.Fatalf("client setup failure was hidden: %v %q", err, output.String())
	}
}

func TestUpgradeCheckBoundsBlockedAPIReads(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	var output bytes.Buffer
	err := runUpgradeCheck(context.Background(), []string{"--namespace=system", "--timeout=50ms"}, &output,
		func() (kubernetes.Interface, dynamic.Interface, error) {
			typed, err := kubernetes.NewForConfig(config)
			if err != nil {
				return nil, nil, err
			}
			dyn, err := dynamic.NewForConfig(config)
			return typed, dyn, err
		})
	if !errors.Is(err, context.DeadlineExceeded) || output.Len() != 0 {
		t.Fatalf("blocked API escaped the inspection deadline: err=%v report=%q", err, output.String())
	}
}

func TestUpgradeCheckClientLoadRequiresClusterConfiguration(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	typed, dyn, err := loadUpgradeCheckClients()
	if err == nil || typed != nil || dyn != nil {
		t.Fatalf("missing in-cluster configuration was accepted: %v", err)
	}
}
