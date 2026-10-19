package helm

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

func TestNodeAdvertisementPolicy(t *testing.T) {
	for _, test := range []struct {
		name, account string
		args          []string
	}{
		{"default", fullname + "-node", nil},
		{"external account", "external-node", []string{"--set", "serviceAccount.node.create=false,serviceAccount.node.name=external-node"}},
		{"RPC disabled", fullname + "-node", []string{"--set", "nodeRPC.enabled=false"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := render(t, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			var policy admissionv1.ValidatingAdmissionPolicy
			var binding admissionv1.ValidatingAdmissionPolicyBinding
			for _, doc := range strings.Split(output, "\n---") {
				var header struct{ Kind string }
				if err := yaml.Unmarshal([]byte(doc), &header); err != nil {
					t.Fatal(err)
				}
				switch header.Kind {
				case "ValidatingAdmissionPolicy":
					if err := yaml.Unmarshal([]byte(doc), &policy); err != nil {
						t.Fatal(err)
					}
				case "ValidatingAdmissionPolicyBinding":
					if err := yaml.Unmarshal([]byte(doc), &binding); err != nil {
						t.Fatal(err)
					}
				}
			}
			if policy.Name == "" || policy.Spec.FailurePolicy == nil || *policy.Spec.FailurePolicy != admissionv1.Fail ||
				binding.Spec.PolicyName != policy.Name || !slices.Equal(binding.Spec.ValidationActions, []admissionv1.ValidationAction{admissionv1.Deny}) {
				t.Fatal("Node advertisement must be enforced by a fail-closed bound policy")
			}
			wantActor := fmt.Sprintf("request.userInfo.username == 'system:serviceaccount:storage:%s'", test.account)
			if len(policy.Spec.MatchConditions) != 1 || policy.Spec.MatchConditions[0].Expression != wantActor {
				t.Fatal("policy is not scoped to the configured Node ServiceAccount")
			}
			if policy.Spec.MatchConstraints == nil || len(policy.Spec.MatchConstraints.ResourceRules) != 1 {
				t.Fatal("policy must constrain only Pod updates")
			}
			rule := policy.Spec.MatchConstraints.ResourceRules[0]
			if !slices.Equal(rule.Resources, []string{"pods"}) || !slices.Equal(rule.Operations, []admissionv1.OperationType{admissionv1.Update}) {
				t.Fatal("policy unexpectedly affects another resource or operation")
			}
		})
	}
	for _, account := range []string{fullname + "-controller", fullname + "-helper"} {
		output, err := render(t, "--set", "serviceAccount.node.name="+account)
		if err == nil || !strings.Contains(output, "Node ServiceAccount must be distinct") {
			t.Fatal("shared Node credentials must fail before changing runtime permissions")
		}
	}
}

func TestNodeStatusPermissionsRequireOnlyUpdate(t *testing.T) {
	output, err := render(t)
	if err != nil {
		t.Fatal(err)
	}
	role := parseChart(t, output).wantObject(t, "native execution status writes", "ClusterRole", fullname+"-node")
	seen := map[string]bool{}
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if strings.HasSuffix(resource, "/status") {
				if !slices.Equal(rule.Verbs, []string{"update"}) {
					t.Errorf("Node %s grants unnecessary status verbs: %v", resource, rule.Verbs)
				}
				seen[resource] = true
			}
		}
	}
	for _, resource := range []string{"shiftpvpools/status", "shiftpvvolumes/status", "shiftpvmoves/status"} {
		if !seen[resource] {
			t.Errorf("Node lost the required receipt/observation update for %s", resource)
		}
	}
}

func workloadSpec(t *testing.T, raw, kind string) corev1.PodSpec {
	t.Helper()
	for _, doc := range strings.Split(raw, "\n---\n") {
		var object struct {
			Kind string `json:"kind"`
			Spec struct {
				Template struct {
					Spec corev1.PodSpec `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &object); err != nil {
			t.Fatal(err)
		}
		if object.Kind == kind {
			return object.Spec.Template.Spec
		}
	}
	t.Fatalf("missing %s", kind)
	return corev1.PodSpec{}
}
func TestNodeRPCUsesDedicatedRotatingTokenAndExactPodPort(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			args := []string{"--set", "serviceAccount.controller.name=rpc-controller"}
			if !enabled {
				args = append(args, "--set", "nodeRPC.enabled=false")
			}
			output, err := render(t, args...)
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			controller := workloadSpec(t, output, "Deployment")
			node := workloadSpec(t, output, "DaemonSet")
			mounted := false
			for _, container := range controller.Containers {
				for _, mount := range container.VolumeMounts {
					if mount.Name == "node-rpc-token" {
						if container.Name != "shiftpv-controller" || !mount.ReadOnly {
							t.Fatal("Node RPC credential exposed to a sidecar")
						}
						mounted = true
					}
				}
			}
			if mounted != enabled {
				t.Fatal("dedicated credential mount does not follow RPC setting")
			}
			projected := false
			for _, volume := range controller.Volumes {
				if volume.Name == "node-rpc-token" {
					if volume.Projected == nil || len(volume.Projected.Sources) != 1 {
						t.Fatal("credential is not projected")
					}
					source := volume.Projected.Sources[0].ServiceAccountToken
					if source == nil || source.Audience != "shiftpv-node" || source.ExpirationSeconds == nil || *source.ExpirationSeconds != 3600 {
						t.Fatal("credential audience/rotation wrong")
					}
					projected = true
				}
			}
			if projected != enabled {
				t.Fatal("credential projection does not follow RPC setting")
			}
			controllerArgs, nodeArgs := controller.Containers[0].Args, node.Containers[0].Args
			if slices.Contains(controllerArgs, "--node-rpc-token-file=/var/run/shiftpv-node-rpc/token") != enabled || slices.Contains(nodeArgs, "--node-rpc-listen-address=:9760") != enabled {
				t.Fatal("RPC flags do not match mounts/ports")
			}
			if enabled && !slices.Contains(nodeArgs, "--controller-service-account=rpc-controller") {
				t.Fatal("Node trusts wrong controller account")
			}
			parsed := parseChart(t, output)
			reviewRights := 0
			for _, object := range parsed.objects {
				for _, rule := range object.Rules {
					if slices.Contains(rule.Resources, "tokenreviews") {
						reviewRights++
						if object.Kind != "ClusterRole" || object.Metadata.Name != fullname+"-node" || !slices.Equal(rule.Verbs, []string{"create"}) {
							t.Fatal("TokenReview rights outside Node authorizer")
						}
					}
				}
			}
			if (reviewRights == 1) != enabled || reviewRights > 1 {
				t.Fatal("unexpected TokenReview rights")
			}
		})
	}
}
func TestNodeRPCRejectsInvalidOrConflictingPorts(t *testing.T) {
	for _, args := range [][]string{{"--set", "nodeRPC.port=0"}, {"--set", "nodeRPC.port=65536"}, {"--set", "nodeRPC.port=9808"}, {"--set", "metrics.enabled=true", "--set", "metrics.port=9760"}} {
		if output, err := render(t, args...); err == nil {
			t.Fatalf("invalid RPC port rendered: %s", output)
		}
	}
}
