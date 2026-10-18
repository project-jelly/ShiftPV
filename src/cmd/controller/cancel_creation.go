package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type creationCancellationLoader func(namespace, daemonSet string) (*creation.Canceller, error)

// Explicit recovery approves only one unfinished initial creation. The shared
// CAS preserves Ready winners and never substitutes normal Ready-volume delete.
func runCancelCreation(ctx context.Context, args []string, output io.Writer, load creationCancellationLoader) error {
	flags := flag.NewFlagSet("cancel-creation", flag.ContinueOnError)
	flags.SetOutput(output)
	id := flags.String("volume-id", "", "exact unfinished initial Volume ID; Ready winners are preserved")
	uid := flags.String("volume-uid", "", "exact Kubernetes Volume UID to cancel")
	namespace := flags.String("namespace", os.Getenv("POD_NAMESPACE"), "ShiftPV system namespace")
	daemonSet := flags.String("node-daemonset-name", "shiftpv-node", "trusted resident Node DaemonSet")
	timeout := flags.Duration("timeout", 90*time.Second, "maximum creation cancellation settlement time")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || volume.ValidateID(*id) != nil || !volume.ValidIdentityToken(*uid) ||
		len(validation.IsDNS1123Label(*namespace)) != 0 || len(validation.IsDNS1123Subdomain(*daemonSet)) != 0 || *timeout <= 0 {
		return fmt.Errorf("exact Volume ID/UID, namespace and Node DaemonSet are required; timeout must be positive; positional arguments are not accepted")
	}
	opCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := opCtx.Err(); err != nil {
		return err
	}
	canceller, err := load(*namespace, *daemonSet)
	if err != nil {
		return fmt.Errorf("initialize creation cancellation: %w", err)
	}
	if canceller == nil || canceller.Volumes == nil {
		return fmt.Errorf("creation cancellation repository is unavailable")
	}
	err = wait.PollUntilContextCancel(opCtx, time.Second, true, func(ctx context.Context) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		complete, err := canceller.Advance(ctx, *id, *uid)
		if err != nil || !complete {
			return false, err
		}
		state, err := canceller.Volumes.Get(ctx, *id)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if state.UID != *uid {
			return false, volumeapi.ErrStateConflict
		}
		return false, fmt.Errorf("%w: remaining Volume protection %v retains its reservation", volumeapi.ErrVolumeFinalizersPending, state.Finalizers)
	})
	if err != nil {
		return fmt.Errorf("settle creation cancellation %s/%s: %w", *id, *uid, err)
	}
	return json.NewEncoder(output).Encode(struct {
		VolumeID  string `json:"volumeID"`
		VolumeUID string `json:"volumeUID"`
		Complete  bool   `json:"complete"`
	}{*id, *uid, true})
}

func loadCreationCancellation(namespace, daemonSet string) (*creation.Canceller, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	config.UserAgent = "shiftpv-cancel-creation"
	config.QPS, config.Burst = 10, 20
	config.RateLimiter = wiring.NewAPIBudget(config.QPS, config.Burst)
	typed, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	api, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return newCreationCanceller(typed, &volumeapi.Registry{Client: api}, namespace, daemonSet), nil
}
