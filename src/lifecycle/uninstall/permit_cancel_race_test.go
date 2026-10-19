package uninstall

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

func TestReportStaleCancelPreservesNewerQuiesceAttempt(t *testing.T) {
	testCancelRace(t, true)
}

func TestCancelRetriesConcurrentAcknowledgement(t *testing.T) {
	testCancelRace(t, false)
}

func testCancelRace(t *testing.T, newAttempt bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store, client := permitFixture()
	first, err := store.BeginQuiesce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resource := corev1.SchemeGroupVersion.WithResource("configmaps")
	state, err := client.CoreV1().ConfigMaps(store.Namespace).Get(ctx, store.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state.UID, state.ResourceVersion = types.UID("permit-uid"), "1"
	if err := client.Tracker().Update(resource, state, store.Namespace); err != nil {
		t.Fatal(err)
	}
	injected := false
	// readState reads the permit before its CSIDriver. Inject a replacement
	// attempt after the old read, preserving the ConfigMap UID as BeginQuiesce does.
	client.PrependReactor("get", "csidrivers", func(clienttesting.Action) (bool, runtime.Object, error) {
		if !injected {
			injected = true
			next := state.DeepCopy()
			next.ResourceVersion = "2"
			if newAttempt {
				next.Data[attemptKey] = "next-attempt"
			} else {
				next.Data[controllerAckKey] = controllerAckDone
			}
			if err := client.Tracker().Update(resource, next, store.Namespace); err != nil {
				return true, nil, err
			}
		}
		return false, nil, nil
	})
	// The stock fake ignores DELETE preconditions. Honor both UID and RV here
	// so the test exercises API compare-and-delete semantics.
	client.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletion := action.(clienttesting.DeleteAction)
		object, err := client.Tracker().Get(resource, store.Namespace, deletion.GetName())
		if err != nil {
			return true, nil, err
		}
		live := object.(*corev1.ConfigMap)
		p := deletion.GetDeleteOptions().Preconditions
		if p != nil && (p.UID != nil && *p.UID != live.UID || p.ResourceVersion != nil && *p.ResourceVersion != live.ResourceVersion) {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, live.Name, fmt.Errorf("delete identity/revision changed"))
		}
		return false, nil, nil
	})
	err = store.Cancel(ctx, first)
	if err != nil && !apierrors.IsConflict(err) {
		t.Fatal(err)
	}
	live, err := client.CoreV1().ConfigMaps(store.Namespace).Get(ctx, store.Name, metav1.GetOptions{})
	if !newAttempt {
		if !apierrors.IsNotFound(err) {
			t.Fatalf("same attempt acknowledgement prevented cancellation: %v", err)
		}
		return
	}
	if err != nil || live.Data[attemptKey] != "next-attempt" {
		t.Fatalf("LC-05 retry boundary: stale cancellation deleted the newer attempt: state=%v error=%v", live, err)
	}
}

type reportRoundTripper func(*http.Request) (*http.Response, error)

func (f reportRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// The same race against a real isolated API server, never an ambient context.
func TestReportNativeStaleCancelPreservesNewerAttempt(t *testing.T) {
	if os.Getenv("SHIFT_PV_REPRO_NATIVE") != "1" {
		t.Skip("requires explicitly selected isolated Kind API server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	path := os.Getenv("KUBECONFIG")
	if !filepath.IsAbs(path) {
		t.Fatal("explicit absolute fixture KUBECONFIG is required")
	}
	data, err := exec.Command("kubectl", "--kubeconfig", path, "config", "view", "--raw", "--minify", "--flatten", "-o", "json").Output()
	if err != nil {
		t.Fatal(err)
	}
	var credentials struct {
		Clusters []struct {
			Cluster struct {
				Server string `json:"server"`
				CA     []byte `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
		Users []struct {
			User struct {
				Certificate []byte `json:"client-certificate-data"`
				Key         []byte `json:"client-key-data"`
			} `json:"user"`
		} `json:"users"`
	}
	if json.Unmarshal(data, &credentials) != nil || len(credentials.Clusters) != 1 || len(credentials.Users) != 1 || len(credentials.Users[0].User.Key) == 0 {
		t.Fatal("fixture requires one selected Kind certificate identity")
	}
	cfg := &rest.Config{Host: credentials.Clusters[0].Cluster.Server, TLSClientConfig: rest.TLSClientConfig{
		CAData: credentials.Clusters[0].Cluster.CA, CertData: credentials.Users[0].User.Certificate, KeyData: credentials.Users[0].User.Key,
	}}
	cfg.Timeout = 10 * time.Second
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	namespace, driverName := "boundary-quiesce-race", "boundary-retry.shiftpv.io"
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
		_ = client.StorageV1().CSIDrivers().Delete(context.Background(), driverName, metav1.DeleteOptions{})
	})
	if _, err := client.StorageV1().CSIDrivers().Create(ctx, &storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: driverName}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	store := &PermitStore{Client: client, Namespace: namespace, Name: "permit", CSIDriver: driverName}
	first, err := store.BeginQuiesce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var next string
	var injected error
	var once sync.Once
	intercept := rest.CopyConfig(cfg)
	intercept.WrapTransport = func(base http.RoundTripper) http.RoundTripper {
		return reportRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodDelete && strings.HasSuffix(request.URL.Path, "/namespaces/"+namespace+"/configmaps/permit") {
				once.Do(func() { next, injected = store.BeginQuiesce(ctx) })
				if injected != nil {
					return nil, injected
				}
			}
			return base.RoundTrip(request)
		})
	}
	interceptClient, err := kubernetes.NewForConfig(intercept)
	if err != nil {
		t.Fatal(err)
	}
	old := *store
	old.Client = interceptClient
	err = old.Cancel(ctx, first)
	if err != nil && !apierrors.IsConflict(err) {
		t.Fatal(err)
	}
	if injected != nil || next == "" || next == first {
		t.Fatalf("new attempt was not created: next=%q injected=%v", next, injected)
	}
	live, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, store.Name, metav1.GetOptions{})
	if err != nil || live.Data[attemptKey] != next {
		t.Fatalf("LC-05 native API race: old Cancel removed a newer quiesce attempt: state=%v error=%v", live, err)
	}
}
