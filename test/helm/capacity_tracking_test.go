package helm

import (
	"slices"
	"strings"
	"testing"
)

func TestCapacityTrackingRender(t *testing.T) {
	for _, tc := range []struct {
		name      string
		values    []string
		enabled   bool
		interval  string
		namespace string
		account   string
	}{
		{name: "default disabled", namespace: "storage", account: fullname + "-controller"},
		{name: "enabled", values: []string{"--set", "capacityTracking.enabled=true"}, enabled: true, interval: "1m", namespace: "storage", account: fullname + "-controller"},
		{
			name: "custom namespace interval and account", enabled: true, interval: "5s", namespace: "custom-storage", account: "external-controller",
			values: []string{"--namespace", "custom-storage", "--set", "capacityTracking.enabled=true,capacityTracking.pollInterval=5s,serviceAccount.controller.create=false,serviceAccount.controller.name=external-controller"},
		},
		{name: "explicitly disabled", values: []string{"--set", "capacityTracking.enabled=false,capacityTracking.pollInterval=1h"}, namespace: "storage", account: fullname + "-controller"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := render(t, tc.values...)
			if err != nil {
				t.Fatalf("render: %v\n%s", err, output)
			}
			c := parseChart(t, output)
			driver := c.wantObject(t, "scheduler gate", "CSIDriver", "csi.shiftpv.io")
			if got := driver.Spec.StorageCapacity; got == nil || *got != tc.enabled {
				t.Errorf("CSIDriver storageCapacity = %v, want explicit %t", got, tc.enabled)
			}
			provisioner := c.container(t, "Deployment", fullname+"-controller", "csi-provisioner").Spec.Template.Spec.Containers[0]
			wantArg(t, "leader election remains enabled", provisioner.Args, "--leader-election=true")
			wantArg(t, "leader election namespace", provisioner.Args, "--leader-election-namespace=$(POD_NAMESPACE)")
			if tc.enabled {
				wantArg(t, "capacity publisher", provisioner.Args, "--enable-capacity=true")
				wantArg(t, "Deployment owner", provisioner.Args, "--capacity-ownerref-level=2")
				wantArg(t, "capacity polling", provisioner.Args, "--capacity-poll-interval="+tc.interval)
			} else {
				wantNoArgPrefix(t, "disabled capacity publisher", provisioner.Args, "--enable-capacity")
				wantNoArgPrefix(t, "disabled capacity options", provisioner.Args, "--capacity-")
			}
			env := map[string]string{}
			for _, variable := range provisioner.Env {
				if _, exists := env[variable.Name]; exists {
					t.Errorf("duplicate provisioner environment variable %q", variable.Name)
				}
				env[variable.Name] = variable.ValueFrom.FieldRef.FieldPath
			}
			if env["POD_NAMESPACE"] != "metadata.namespace" {
				t.Errorf("leader namespace env = %q", env["POD_NAMESPACE"])
			}
			for name, fieldPath := range map[string]string{"NAMESPACE": "metadata.namespace", "POD_NAME": "metadata.name"} {
				got, exists := env[name]
				if tc.enabled && got != fieldPath || !tc.enabled && exists {
					t.Errorf("capacity environment %s = %q (present=%t, enabled=%t)", name, got, exists, tc.enabled)
				}
			}

			capacityRules := 0
			for _, object := range c.objects {
				for _, rule := range object.Rules {
					if !slices.Contains(rule.Resources, "csistoragecapacities") {
						continue
					}
					capacityRules++
					if object.Kind != "Role" || object.Metadata.Name != fullname+"-controller" || object.Metadata.Namespace != tc.namespace {
						t.Errorf("capacity permissions must belong to the controller Role in %s, got %s %s/%s", tc.namespace, object.Kind, object.Metadata.Namespace, object.Metadata.Name)
					}
					if !slices.Equal(rule.APIGroups, []string{"storage.k8s.io"}) || !slices.Equal(rule.Resources, []string{"csistoragecapacities"}) {
						t.Errorf("unexpected capacity permission scope: %+v", rule)
					}
					verbs := slices.Clone(rule.Verbs)
					slices.Sort(verbs)
					if !slices.Equal(verbs, []string{"create", "delete", "get", "list", "patch", "update", "watch"}) {
						t.Errorf("capacity verbs = %v", rule.Verbs)
					}
				}
			}
			wantRules := 0
			if tc.enabled {
				wantRules = 1
			}
			if capacityRules != wantRules {
				t.Errorf("capacity RBAC rules = %d, want %d", capacityRules, wantRules)
			}
			if tc.enabled {
				role := c.wantObject(t, "Deployment owner lookup", "ClusterRole", fullname+"-provisioner")
				for resource, group := range map[string]string{"pods": "", "replicasets": "apps"} {
					allowed := false
					for _, rule := range role.Rules {
						allowed = allowed || slices.Contains(rule.Resources, resource) && slices.Contains(rule.APIGroups, group) && slices.Contains(rule.Verbs, "get")
					}
					if !allowed {
						t.Errorf("capacity owner lookup needs get on %s", resource)
					}
				}
			}
			deployment := c.wantObject(t, "controller account", "Deployment", fullname+"-controller")
			if deployment.Spec.Template.Spec.ServiceAccountName != tc.account {
				t.Errorf("controller account = %q, want %q", deployment.Spec.Template.Spec.ServiceAccountName, tc.account)
			}
			binding := c.wantObject(t, "capacity binding", "RoleBinding", fullname+"-controller")
			if binding.Metadata.Namespace != tc.namespace || binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != fullname+"-controller" {
				t.Errorf("unexpected controller RoleBinding: %+v", binding)
			}
			if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != tc.account || binding.Subjects[0].Namespace != tc.namespace {
				t.Errorf("controller RoleBinding subjects = %+v", binding.Subjects)
			}
		})
	}
}

func TestCapacityTrackingRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{
		"capacityTracking.pollInterval=0s",
		"capacityTracking.pollInterval=-1m",
		"capacityTracking.pollInterval=soon",
		"capacityTracking.pollInterval=1",
		"capacityTracking.enabled=yes",
		"capacityTracking.pollIntervall=5s",
	} {
		t.Run(value, func(t *testing.T) {
			output, err := render(t, "--set", value)
			if err == nil {
				t.Fatalf("invalid capacity setting accepted: %s", value)
			}
			if !strings.Contains(output, "capacityTracking") {
				t.Fatalf("render failed without capacity setting diagnostic: %v\n%s", err, output)
			}
		})
	}
}

func TestMaintenanceWriterObservationIsScopedToInstalledDeployment(t *testing.T) {
	for _, name := range []string{fullname, "custom-storage"} {
		output, err := render(t, "--namespace", "storage", "--set", "fullnameOverride="+name)
		if err != nil {
			t.Fatal(err, output)
		}
		found := 0
		for _, object := range parseChart(t, output).objects {
			for _, rule := range object.Rules {
				if !slices.Contains(rule.Resources, "deployments") {
					continue
				}
				found++
				if object.Kind != "Role" || object.Metadata.Namespace != "storage" || object.Metadata.Name != name+"-controller" ||
					!slices.Equal(rule.APIGroups, []string{"apps"}) || !slices.Equal(rule.Resources, []string{"deployments"}) ||
					!slices.Equal(rule.Verbs, []string{"get"}) || !slices.Equal(rule.ResourceNames, []string{name + "-controller"}) {
					t.Fatalf("maintenance can observe unrelated Deployments: %+v %+v", object.Metadata, rule)
				}
			}
		}
		if found != 1 {
			t.Fatalf("expected one namespaced exact-Deployment observation rule, got %d", found)
		}
	}
}
