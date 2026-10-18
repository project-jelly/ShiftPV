package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	mobilitycontroller "github.com/project-jelly/ShiftPV/src/mobility/controller"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type retiredMoveOptions struct {
	namespace, daemonSet, deployment, helperImage, serviceAccount string
}

type retiredMoveAdvancer interface {
	AdvanceRetiredMove(context.Context, string, string) (bool, error)
}

type retiredMoveRuntime struct {
	client kubernetes.Interface
	moves  retiredMoveAdvancer
}

func runSettleRetiredMove(ctx context.Context, args []string, output io.Writer, load func(retiredMoveOptions) (retiredMoveRuntime, error)) error {
	flags := flag.NewFlagSet("settle-retired-move", flag.ContinueOnError)
	flags.SetOutput(output)
	name := flags.String("move-name", "", "exact prepared-v1 Move whose original claim has retired")
	uid := flags.String("move-uid", "", "exact Kubernetes Move UID")
	var opts retiredMoveOptions
	flags.StringVar(&opts.namespace, "namespace", os.Getenv("POD_NAMESPACE"), "ShiftPV system namespace")
	flags.StringVar(&opts.daemonSet, "node-daemonset-name", "shiftpv-node", "trusted resident Node DaemonSet")
	flags.StringVar(&opts.deployment, "controller-deployment-name", "shiftpv-controller", "normal writer Deployment, required stopped")
	flags.StringVar(&opts.helperImage, "helper-image", "", "candidate controller image for owner verification helpers")
	flags.StringVar(&opts.serviceAccount, "helper-service-account", "", "trusted controller service account for helpers")
	timeout := flags.Duration("timeout", 5*time.Minute, "maximum retirement settlement time")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !volume.ValidObjectName(*name) || !volume.ValidIdentityToken(*uid) ||
		len(validation.IsDNS1123Label(opts.namespace)) != 0 || len(validation.IsDNS1123Subdomain(opts.daemonSet)) != 0 ||
		len(validation.IsDNS1123Subdomain(opts.deployment)) != 0 || len(validation.IsDNS1123Subdomain(opts.serviceAccount)) != 0 ||
		strings.TrimSpace(opts.helperImage) == "" || *timeout <= 0 {
		return fmt.Errorf("exact Move name/UID, namespace, Node DaemonSet, stopped controller Deployment, helper image and service account are required; timeout must be positive; positional arguments are not accepted")
	}
	opCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := opCtx.Err(); err != nil {
		return err
	}
	runtime, err := load(opts)
	if err != nil {
		return fmt.Errorf("initialize retired Move settlement: %w", err)
	}
	if runtime.client == nil || runtime.moves == nil {
		return fmt.Errorf("retired Move settlement dependencies are unavailable")
	}
	var lastErr error
	err = wait.PollUntilContextCancel(opCtx, time.Second, true, func(ctx context.Context) (bool, error) {
		if err := normalWriterStopped(ctx, runtime.client, opts); err != nil {
			return false, err
		}
		complete, stepErr := runtime.moves.AdvanceRetiredMove(ctx, *name, *uid)
		if errors.Is(stepErr, mobilitycontroller.ErrRetiredMoveRejected) {
			return false, stepErr
		}
		if stepErr != nil {
			// Missing observations and pending effects retain their durable work.
			// Reobserve with a bounded deadline, just as the background reconciler.
			lastErr = stepErr
			return false, nil
		}
		lastErr = nil
		return complete, nil
	})
	if err != nil {
		return fmt.Errorf("settle retired Move %s/%s: %w", *name, *uid, errors.Join(err, lastErr))
	}
	return json.NewEncoder(output).Encode(struct {
		MoveName string `json:"moveName"`
		MoveUID  string `json:"moveUID"`
		Complete bool   `json:"complete"`
	}{*name, *uid, true})
}

// This observation is not a distributed lock. Operators must keep GitOps/HPA
// and other writers quiesced for the entire maintenance interval.
func normalWriterStopped(ctx context.Context, client kubernetes.Interface, opts retiredMoveOptions) error {
	deployment, err := client.AppsV1().Deployments(opts.namespace).Get(ctx, opts.deployment, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("observe stopped controller Deployment: %w", err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 || deployment.Spec.Selector == nil {
		return fmt.Errorf("normal controller Deployment must remain scaled to zero")
	}
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil || selector.Empty() {
		return fmt.Errorf("normal controller Deployment has no valid nonempty Pod selector")
	}
	pods, err := client.CoreV1().Pods(opts.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return fmt.Errorf("observe stopped controller Pods: %w", err)
	}
	if len(pods.Items) != 0 {
		return fmt.Errorf("normal controller Pods must be absent before retirement settlement")
	}
	return nil
}

func loadRetiredMoveRuntime(opts retiredMoveOptions) (retiredMoveRuntime, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return retiredMoveRuntime{}, err
	}
	config.UserAgent, config.QPS, config.Burst = "shiftpv-settle-retired-move", 10, 20
	config.RateLimiter = wiring.NewAPIBudget(config.QPS, config.Burst)
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return retiredMoveRuntime{}, err
	}
	api, err := dynamic.NewForConfig(config)
	if err != nil {
		return retiredMoveRuntime{}, err
	}
	volumes := &volumeapi.Registry{Client: api}
	inspector := &helperpod.Inspector{Client: client, Namespace: opts.namespace}
	moves := &mobilitycontroller.Reconciler{
		Client: client, Repository: volumes, Namespace: opts.namespace,
		HelperImage: opts.helperImage, ServiceAccountName: opts.serviceAccount,
		SourceUsage: &helperpod.Runner{Client: client, Namespace: opts.namespace},
		PoolLocks:   &poolcapacity.Locker{}, Cleanups: &cleanupapi.Store{Client: api}, HistoryInspector: inspector,
		CleanupOperator: &nodeexecutor.Client{Volumes: volumes, Inspector: inspector,
			Discovery: nodeexecutor.Discovery{Client: client, Namespace: opts.namespace, DaemonSet: opts.daemonSet}},
	}
	return retiredMoveRuntime{client: client, moves: moves}, nil
}
