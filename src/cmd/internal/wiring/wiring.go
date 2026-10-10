// Package wiring holds the Kubernetes client construction and bootstrap retry
// every ShiftPV process entrypoint repeats. It owns no failure convention:
// each binary passes its own Fail and keeps reporting startup failures exactly
// as it did before.
package wiring

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// Fail reports one wiring step failure in the calling binary's own convention.
// It must terminate the process; a Fail that returns aborts the wiring with
// zero values rather than continuing with an unusable client.
type Fail func(step string, err error)

// Clients are the Kubernetes clients one process wires from a single REST
// configuration.
type Clients struct {
	Config  *rest.Config
	Typed   *kubernetes.Clientset
	Dynamic *dynamic.DynamicClient
}

// InCluster loads the in-cluster configuration and builds the typed and
// dynamic clients from it.
func InCluster(fail Fail) Clients {
	config, ok := inClusterConfig(fail)
	if !ok {
		return Clients{}
	}
	return ForConfig(config, "", fail)
}

// InClusterDynamic loads the in-cluster configuration and builds only the
// dynamic client, for a process that needs no typed client.
func InClusterDynamic(fail Fail) *dynamic.DynamicClient {
	config, ok := inClusterConfig(fail)
	if !ok {
		return nil
	}
	return DynamicForConfig(config, "", fail)
}

// InClusterConfig loads configuration without constructing clients.
func InClusterConfig(fail Fail) *rest.Config {
	config, _ := inClusterConfig(fail)
	return config
}

// DynamicForConfig builds a dynamic-only client with the supplied role budget.
func DynamicForConfig(config *rest.Config, role string, fail Fail) *dynamic.DynamicClient {
	qualifier := ""
	if role != "" {
		qualifier = role + " "
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		fail("create "+qualifier+"dynamic Kubernetes client", err)
		return nil
	}
	return dynamicClient
}

// ForConfig builds both clients from an already-loaded configuration. role
// qualifies the failure step names for a process that wires more than one
// client pair; an empty role leaves them unqualified.
func ForConfig(config *rest.Config, role string, fail Fail) Clients {
	qualifier := ""
	if role != "" {
		qualifier = role + " "
	}
	typed, err := kubernetes.NewForConfig(config)
	if err != nil {
		fail("create "+qualifier+"Kubernetes client", err)
		return Clients{}
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		fail("create "+qualifier+"dynamic Kubernetes client", err)
		return Clients{}
	}
	return Clients{Config: config, Typed: typed, Dynamic: dynamicClient}
}

func inClusterConfig(fail Fail) (*rest.Config, bool) {
	config, err := rest.InClusterConfig()
	if err != nil {
		fail("load in-cluster configuration", err)
		return nil, false
	}
	return config, true
}

// BootstrapWithRetry drives one bootstrap step to success on the shared
// one-second, two-minute budget, logging each transient failure under name.
func BootstrapWithRetry(ctx context.Context, name string, bootstrap func(context.Context) error) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := bootstrap(ctx); err != nil {
			klog.V(2).Infof("waiting for %s: %v", name, err)
			return false, nil
		}
		return true, nil
	})
}
