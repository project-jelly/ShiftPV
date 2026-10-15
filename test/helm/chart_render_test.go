package helm

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// render() installs the chart as release "test", so every derived name carries
// the "test-shiftpv" fullname prefix. Names that come from values instead of the
// release (the StorageClass, the CSI driver) stay unprefixed.
const fullname = "test-shiftpv"

func TestContainerSecurityContextRequiresRunAsUserKey(t *testing.T) {
	chartDir := filepath.Join(t.TempDir(), "shiftpv")
	if err := os.CopyFS(chartDir, os.DirFS("../../charts/shiftpv")); err != nil {
		t.Fatalf("copy chart fixture: %v", err)
	}
	deploymentPath := filepath.Join(chartDir, "templates", "controller", "deployment.yaml")
	raw, err := os.ReadFile(deploymentPath)
	if err != nil {
		t.Fatal(err)
	}
	withUser := `(dict "runAsNonRoot" true "runAsUser" 65532)`
	withoutUser := `(dict "runAsNonRoot" true)`
	changed := strings.Replace(string(raw), withUser, withoutUser, 1)
	if changed == string(raw) {
		t.Fatalf("controller deployment has no security context invocation to exercise")
	}
	if err := os.WriteFile(deploymentPath, []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("helm", "template", "test", chartDir, "--namespace", "storage", "--kube-version", "1.35.8")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("render without runAsUser unexpectedly succeeded")
	}
	if !strings.Contains(string(output), "containerSecurityContext needs runAsUser") {
		t.Fatalf("render failed without the missing-key diagnostic: %v\n%s", err, output)
	}
}

type renderedObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name        string            `json:"name"`
		Namespace   string            `json:"namespace"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Rules []struct {
		APIGroups []string `json:"apiGroups"`
		Resources []string `json:"resources"`
		Verbs     []string `json:"verbs"`
	} `json:"rules"`
	Subjects []struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"subjects"`
	RoleRef struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"roleRef"`
	Spec struct {
		StorageCapacity *bool `json:"storageCapacity"`
		Template        struct {
			Spec struct {
				ServiceAccountName string `json:"serviceAccountName"`
				Containers         []struct {
					Name    string   `json:"name"`
					Image   string   `json:"image"`
					Command []string `json:"command"`
					Args    []string `json:"args"`
					Env     []struct {
						Name      string `json:"name"`
						ValueFrom struct {
							FieldRef struct {
								FieldPath string `json:"fieldPath"`
							} `json:"fieldRef"`
						} `json:"valueFrom"`
					} `json:"env"`
					VolumeMounts []struct {
						Name             string `json:"name"`
						MountPath        string `json:"mountPath"`
						MountPropagation string `json:"mountPropagation"`
					} `json:"volumeMounts"`
				} `json:"containers"`
				Volumes []struct {
					Name     string `json:"name"`
					HostPath struct {
						Path string `json:"path"`
						Type string `json:"type"`
					} `json:"hostPath"`
				} `json:"volumes"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

// chart holds one rendered release: the raw manifest plus every document parsed
// into a structured object, so assertions can name a kind/field instead of
// grepping free text.
type chart struct {
	raw     string
	objects []renderedObject
}

func parseChart(t *testing.T, raw string) chart {
	t.Helper()
	parsed := chart{raw: raw}
	for _, doc := range strings.Split(raw, "\n---\n") {
		if strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(doc), "---")) == "" {
			continue
		}
		var object renderedObject
		if err := yaml.Unmarshal([]byte(doc), &object); err != nil {
			t.Fatalf("undecodable document: %v\n%s", err, doc)
		}
		if object.Kind == "" {
			continue
		}
		parsed.objects = append(parsed.objects, object)
	}
	if len(parsed.objects) == 0 {
		t.Fatal("render produced no objects")
	}
	return parsed
}

func (c chart) find(kind, name string) (renderedObject, bool) {
	for _, object := range c.objects {
		if object.Kind == kind && object.Metadata.Name == name {
			return object, true
		}
	}
	return renderedObject{}, false
}

// wantObject asserts a kind/name pair exists.
func (c chart) wantObject(t *testing.T, assertion, kind, name string) renderedObject {
	t.Helper()
	object, ok := c.find(kind, name)
	if !ok {
		t.Errorf("%s: no %s named %q in the rendered release", assertion, kind, name)
	}
	return object
}

// wantNoObject asserts a kind/name pair is absent; an empty name means the kind
// must not appear at all.
func (c chart) wantNoObject(t *testing.T, assertion, kind, name string) {
	t.Helper()
	for _, object := range c.objects {
		if object.Kind == kind && (name == "" || object.Metadata.Name == name) {
			t.Errorf("%s: unexpected %s named %q", assertion, kind, object.Metadata.Name)
		}
	}
}

func (c chart) wantKind(t *testing.T, assertion, kind string) {
	t.Helper()
	for _, object := range c.objects {
		if object.Kind == kind {
			return
		}
	}
	t.Errorf("%s: no %s in the rendered release", assertion, kind)
}

// container returns the named container of the named workload's pod template.
func (c chart) container(t *testing.T, kind, workload, container string) renderedObject {
	t.Helper()
	object, ok := c.find(kind, workload)
	if !ok {
		t.Fatalf("no %s named %q to read container %q from", kind, workload, container)
	}
	for _, candidate := range object.Spec.Template.Spec.Containers {
		if candidate.Name == container {
			var only renderedObject
			only.Kind = kind
			only.Metadata = object.Metadata
			only.Spec.Template.Spec.Containers = append(only.Spec.Template.Spec.Containers, candidate)
			only.Spec.Template.Spec.Volumes = object.Spec.Template.Spec.Volumes
			return only
		}
	}
	t.Fatalf("%s %q has no container %q", kind, workload, container)
	return renderedObject{}
}

func (c chart) controllerArgs(t *testing.T) []string {
	t.Helper()
	return c.container(t, "Deployment", fullname+"-controller", "shiftpv-controller").Spec.Template.Spec.Containers[0].Args
}

func (c chart) guardContainer(t *testing.T) renderedObject {
	t.Helper()
	return c.container(t, "Job", fullname+"-uninstall-guard", "uninstall-guard")
}

func wantArg(t *testing.T, assertion string, args []string, want string) {
	t.Helper()
	for _, arg := range args {
		if arg == want {
			return
		}
	}
	t.Errorf("%s: missing arg %q, got %v", assertion, want, args)
}

func wantNoArgPrefix(t *testing.T, assertion string, args []string, prefix string) {
	t.Helper()
	for _, arg := range args {
		if strings.HasPrefix(arg, prefix) {
			t.Errorf("%s: unexpected arg %q", assertion, arg)
		}
	}
}

func wantImage(t *testing.T, assertion string, container renderedObject, want string) {
	t.Helper()
	if got := container.Spec.Template.Spec.Containers[0].Image; got != want {
		t.Errorf("%s: image is %q, want %q", assertion, got, want)
	}
}

func wantCommand(t *testing.T, assertion string, container renderedObject, want string) {
	t.Helper()
	command := container.Spec.Template.Spec.Containers[0].Command
	if len(command) != 1 || command[0] != want {
		t.Errorf("%s: command is %v, want [%q]", assertion, command, want)
	}
}

func wantAnnotation(t *testing.T, assertion string, object renderedObject, key, want string) {
	t.Helper()
	if got := object.Metadata.Annotations[key]; got != want {
		t.Errorf("%s: annotation %q is %q, want %q", assertion, key, got, want)
	}
}

func wantNoAnnotation(t *testing.T, assertion string, object renderedObject, key string) {
	t.Helper()
	if got, ok := object.Metadata.Annotations[key]; ok {
		t.Errorf("%s: unexpected annotation %q = %q", assertion, key, got)
	}
}

// wantRule asserts some RBAC rule of the object grants exactly the given
// resources.
func wantRule(t *testing.T, assertion string, object renderedObject, resources ...string) {
	t.Helper()
	want := strings.Join(resources, ",")
	for _, rule := range object.Rules {
		if strings.Join(rule.Resources, ",") == want {
			return
		}
	}
	t.Errorf("%s: %s %q has no rule for resources [%s]", assertion, object.Kind, object.Metadata.Name, want)
}

// wantNoResource asserts no RBAC rule anywhere in the release mentions the
// resource.
func (c chart) wantNoResource(t *testing.T, assertion, resource string) {
	t.Helper()
	for _, object := range c.objects {
		for _, rule := range object.Rules {
			for _, granted := range rule.Resources {
				if strings.HasPrefix(granted, resource) {
					t.Errorf("%s: %s %q still grants %q", assertion, object.Kind, object.Metadata.Name, granted)
				}
			}
		}
	}
}

func wantSubject(t *testing.T, assertion string, object renderedObject, name string) {
	t.Helper()
	for _, subject := range object.Subjects {
		if subject.Kind == "ServiceAccount" && subject.Name == name {
			return
		}
	}
	t.Errorf("%s: %s %q does not bind ServiceAccount %q", assertion, object.Kind, object.Metadata.Name, name)
}

func wantMountPropagation(t *testing.T, assertion string, container renderedObject, mountPath, want string) {
	t.Helper()
	for _, mount := range container.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == mountPath {
			if mount.MountPropagation != want {
				t.Errorf("%s: mount %q propagation is %q, want %q", assertion, mountPath, mount.MountPropagation, want)
			}
			return
		}
	}
	t.Errorf("%s: no volumeMount at %q", assertion, mountPath)
}

func wantHostPath(t *testing.T, assertion string, object renderedObject, volume, want string) {
	t.Helper()
	for _, candidate := range object.Spec.Template.Spec.Volumes {
		if candidate.Name == volume {
			if candidate.HostPath.Path != want {
				t.Errorf("%s: volume %q hostPath is %q, want %q", assertion, volume, candidate.HostPath.Path, want)
			}
			return
		}
	}
	t.Errorf("%s: no volume named %q", assertion, volume)
}

// controllerImageTag reads the default controller tag from values.yaml: the chart
// version is independent of the component version, so Chart.yaml carries no
// application version to derive it from.
func controllerImageTag(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../charts/shiftpv/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Controller struct {
			Image struct {
				Tag string `json:"tag"`
			} `json:"image"`
		} `json:"controller"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if values.Controller.Image.Tag == "" {
		t.Fatal("values.yaml has no controller.image.tag")
	}
	return values.Controller.Image.Tag
}

// TestChartRender covers the chart contracts that make helm-template used to
// assert with grep over raw YAML: one subtest per --set combination, one named
// assertion per contract.
func TestChartRender(t *testing.T) {
	const (
		controllerDeployment = fullname + "-controller"
		nodeDaemonSet        = fullname + "-node"
		guardJob             = fullname + "-uninstall-guard"
		helperName           = fullname + "-helper"
		externalHelper       = "existing-helper"
		microk8sKubeletRoot  = "/var/snap/microk8s/common/var/lib/kubelet"
	)

	for _, tc := range []struct {
		name    string
		args    []string
		failure string
		assert  func(t *testing.T, c chart)
	}{
		{
			name: "default images and service accounts",
			assert: func(t *testing.T, c chart) {
				defaultImage := "ghcr.io/project-jelly/shiftpv-controller:" + controllerImageTag(t)
				args := c.controllerArgs(t)
				wantArg(t, "helper image tracks the controller tag", args, "--helper-image="+defaultImage)
				wantArg(t, "mobility helper image tracks the controller tag", args, "--mobility-helper-image="+defaultImage)
				wantArg(t, "helper service account wiring", args, "--helper-service-account="+helperName)
				wantArg(t, "controller service account wiring", args, "--controller-service-account="+fullname+"-controller")
				c.wantObject(t, "helper service account is created", "ServiceAccount", helperName)
			},
		},
		{
			name:    "rejects a second controller replica",
			args:    []string{"--set", "controller.replicas=2"},
			failure: "controller.replicas",
		},
		{
			name: "overridden images",
			args: []string{"--set", "controller.image.repository=controller,controller.image.tag=test," +
				"node.image.repository=node,node.image.tag=test," +
				"helperPod.image=controller:test,mobility.helperImage=helper:test"},
			assert: func(t *testing.T, c chart) {
				args := c.controllerArgs(t)
				wantImage(t, "controller image override", c.container(t, "Deployment", controllerDeployment, "shiftpv-controller"), "controller:test")
				wantImage(t, "node image override", c.container(t, "DaemonSet", nodeDaemonSet, "shiftpv-node"), "node:test")
				wantArg(t, "helper image override", args, "--helper-image=controller:test")
				wantArg(t, "helper service account wiring", args, "--helper-service-account="+helperName)
				wantArg(t, "controller service account wiring", args, "--controller-service-account="+fullname+"-controller")
				c.wantObject(t, "helper service account is created", "ServiceAccount", helperName)
				wantRule(t, "helper may only write volume and move status",
					c.wantObject(t, "helper cluster role", "ClusterRole", helperName),
					"shiftpvvolumes/status", "shiftpvmoves/status")
				c.wantNoResource(t, "the retired cleanup CRD is gone", "shiftpvcleanups")
				guard := c.wantObject(t, "uninstall guard job", "Job", guardJob)
				wantAnnotation(t, "helm uninstall hook", guard, "helm.sh/hook", "pre-delete")
				wantNoAnnotation(t, "no argocd hook in helm mode", guard, "argocd.argoproj.io/hook")
				wantCommand(t, "uninstall guard entrypoint", c.guardContainer(t), "/shiftpv-uninstall-guard")
				wantArg(t, "mobility helper image override", args, "--mobility-helper-image=helper:test")
				wantArg(t, "webhook service wiring", args, "--webhook-service-name="+fullname+"-webhook")
				wantArg(t, "webhook tls secret wiring", args, "--webhook-tls-secret-name="+fullname+"-webhook-tls")
				wantArg(t, "mutating webhook configuration wiring", args, "--webhook-configuration-name="+fullname+"-mobility")
				wantArg(t, "validating webhook configuration wiring", args, "--validation-webhook-configuration-name="+fullname+"-lifecycle")
				wantArg(t, "storage class wiring", args, "--storage-class-name=shiftpv")
				wantArg(t, "uninstall permit wiring", args, "--uninstall-permit-name="+fullname+"-uninstall-permit")
				guardArgs := c.guardContainer(t).Spec.Template.Spec.Containers[0].Args
				wantArg(t, "guard reads the same permit", guardArgs, "--permit-name="+fullname+"-uninstall-permit")
				wantArg(t, "guard reads the same validating webhook", guardArgs, "--validation-webhook="+fullname+"-lifecycle")
				c.wantNoObject(t, "the controller owns the mutating webhook, not the chart", "MutatingWebhookConfiguration", "")
				c.wantNoObject(t, "the controller owns the validating webhook, not the chart", "ValidatingWebhookConfiguration", "")
				c.wantNoObject(t, "the chart ships no secret", "Secret", "")
				wantMountPropagation(t, "host root is read-only propagated",
					c.container(t, "DaemonSet", nodeDaemonSet, "shiftpv-node"), "/host", "HostToContainer")
			},
		},
		{
			name: "separate create and move helper images",
			args: []string{"--set", "controller.image.repository=controller,controller.image.tag=test," +
				"helperPod.image=create-helper:test,mobility.helperImage=move-helper:test"},
			assert: func(t *testing.T, c chart) {
				args := c.controllerArgs(t)
				wantArg(t, "create helper image", args, "--helper-image=create-helper:test")
				wantArg(t, "move helper image", args, "--mobility-helper-image=move-helper:test")
			},
		},
		{
			name: "externally managed helper service account",
			args: []string{"--set", "serviceAccount.helper.create=false,serviceAccount.helper.name=" + externalHelper},
			assert: func(t *testing.T, c chart) {
				clusterRoleBinding := c.wantObject(t, "helper cluster role binding is still rendered", "ClusterRoleBinding", helperName)
				roleBinding := c.wantObject(t, "helper role binding is still rendered", "RoleBinding", helperName)
				c.wantObject(t, "helper cluster role is still rendered", "ClusterRole", helperName)
				c.wantObject(t, "helper role is still rendered", "Role", helperName)
				c.wantNoObject(t, "chart does not create the helper service account", "ServiceAccount", helperName)
				c.wantNoObject(t, "chart does not create the external service account either", "ServiceAccount", externalHelper)
				wantArg(t, "controller uses the external helper account", c.controllerArgs(t), "--helper-service-account="+externalHelper)
				wantSubject(t, "cluster role binding targets the external account", clusterRoleBinding, externalHelper)
				wantSubject(t, "role binding targets the external account", roleBinding, externalHelper)
			},
		},
		{
			name: "argocd uninstall mode",
			args: []string{"--set", "lifecycle.uninstallMode=argocd"},
			assert: func(t *testing.T, c chart) {
				guard := c.wantObject(t, "uninstall guard job", "Job", guardJob)
				wantAnnotation(t, "argocd pre-delete hook", guard, "argocd.argoproj.io/hook", "PreDelete")
				wantNoAnnotation(t, "no helm hook in argocd mode", guard, "helm.sh/hook")
			},
		},
		{
			name: "mobility disabled",
			args: []string{"--set", "mobility.enabled=false"},
			assert: func(t *testing.T, c chart) {
				args := c.controllerArgs(t)
				c.wantKind(t, "webhook service survives", "Service")
				c.wantObject(t, "webhook service keeps its name", "Service", fullname+"-webhook")
				wantArg(t, "mobility is off", args, "--mobility-enabled=false")
				wantArg(t, "webhook listener still serves health", args, "--webhook-listen-address=:9443")
				wantNoArgPrefix(t, "no move helper image without mobility", args, "--mobility-helper-image=")
			},
		},
		{
			name: "microk8s kubelet root",
			args: []string{"--set", "node.kubeletRootDir=" + microk8sKubeletRoot},
			assert: func(t *testing.T, c chart) {
				node := c.container(t, "DaemonSet", nodeDaemonSet, "shiftpv-node")
				registrar := c.container(t, "DaemonSet", nodeDaemonSet, "node-driver-registrar")
				wantArg(t, "node publishes under the microk8s kubelet root",
					node.Spec.Template.Spec.Containers[0].Args, "--target-root="+microk8sKubeletRoot+"/pods")
				wantArg(t, "registrar advertises the microk8s socket path",
					registrar.Spec.Template.Spec.Containers[0].Args,
					"--kubelet-registration-path="+microk8sKubeletRoot+"/plugins/csi.shiftpv.io/csi.sock")
				daemonSet := c.wantObject(t, "node daemonset", "DaemonSet", nodeDaemonSet)
				wantHostPath(t, "plugin dir follows the kubelet root", daemonSet, "plugin-dir", microk8sKubeletRoot+"/plugins/csi.shiftpv.io")
				wantHostPath(t, "registration dir follows the kubelet root", daemonSet, "registration-dir", microk8sKubeletRoot+"/plugins_registry")
				wantHostPath(t, "kubelet pods dir follows the kubelet root", daemonSet, "kubelet-pods", microk8sKubeletRoot+"/pods")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := render(t, tc.args...)
			if tc.failure != "" {
				if err == nil || !strings.Contains(output, tc.failure) {
					t.Fatalf("wanted failure %q: %v %s", tc.failure, err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			tc.assert(t, parseChart(t, output))
			again, err := render(t, tc.args...)
			if err != nil || again != output {
				t.Fatal("nondeterministic template")
			}
		})
	}
}
