package helm

import (
	"errors"
	"io"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
)

type renderedStorageClass struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Provisioner       string            `json:"provisioner"`
	ReclaimPolicy     string            `json:"reclaimPolicy"`
	VolumeBindingMode string            `json:"volumeBindingMode"`
	Parameters        map[string]string `json:"parameters"`
}

func TestCSIDriverSupportsPodFSGroup(t *testing.T) {
	output, err := render(t)
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if strings.Count(output, "kind: CSIDriver") != 1 ||
		strings.Count(output, "fsGroupPolicy: File") != 1 {
		t.Fatal("CSIDriver must delegate fsGroup ownership changes to kubelet")
	}
}

func TestDefaultStorageClassesExpressDeleteAndRetainLifecycles(t *testing.T) {
	output, err := render(t)
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}

	classes := map[string]renderedStorageClass{}
	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(output), 4096)
	for {
		var object renderedStorageClass
		if err := decoder.Decode(&object); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if object.Kind == "StorageClass" {
			classes[object.Metadata.Name] = object
		}
	}

	if len(classes) != 2 {
		t.Fatalf("wanted exactly two StorageClasses, got %v", classes)
	}
	for name, want := range map[string]struct {
		policy, defaultClass string
	}{
		"shiftpv":        {policy: "Delete", defaultClass: "false"},
		"shiftpv-retain": {policy: "Retain", defaultClass: "false"},
	} {
		class, ok := classes[name]
		if !ok {
			t.Fatalf("missing StorageClass %q", name)
		}
		if class.ReclaimPolicy != want.policy ||
			class.Metadata.Annotations["storageclass.kubernetes.io/is-default-class"] != want.defaultClass ||
			class.Provisioner != "csi.shiftpv.io" ||
			class.VolumeBindingMode != "WaitForFirstConsumer" ||
			class.Parameters["shiftpv.io/capacity-enforcement"] != "none" {
			t.Fatalf("unexpected StorageClass %q: %+v", name, class)
		}
	}
}

func TestDefaultStorageClassRequiresExplicitOptIn(t *testing.T) {
	output, err := render(t, "--set", "storageClass.defaultClass=true",
		"--show-only", "templates/storage/storageclass.yaml")
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if strings.Count(output, `storageclass.kubernetes.io/is-default-class: "true"`) != 1 {
		t.Fatal("explicit opt-in must mark exactly one ShiftPV StorageClass as default")
	}
}

func TestStorageClassLifecyclePoliciesCannotBeSwapped(t *testing.T) {
	for _, setting := range []string{
		"storageClass.reclaimPolicy=Retain",
		"retainStorageClass.reclaimPolicy=Delete",
		"retainStorageClass.defaultClass=true",
	} {
		t.Run(setting, func(t *testing.T) {
			output, err := render(t, "--set", setting)
			if err == nil || !strings.Contains(output, "schema") {
				t.Fatalf("wanted schema rejection for %q: %v %s", setting, err, output)
			}
		})
	}
}

func TestStorageClassNamesMustDiffer(t *testing.T) {
	output, err := render(t, "--set", "retainStorageClass.name=shiftpv")
	if err == nil || !strings.Contains(output, "must differ") {
		t.Fatalf("wanted duplicate StorageClass names to fail: %v %s", err, output)
	}
}

func TestBothStorageClassesAreLifecycleProtected(t *testing.T) {
	output, err := render(t)
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	lines := map[string]int{}
	for _, line := range strings.Split(output, "\n") {
		lines[strings.TrimSpace(line)]++
	}
	for _, flag := range []string{
		"- --storage-class-name=shiftpv",
		"- --storage-class-name=shiftpv-retain",
		"- --storage-class=shiftpv",
		"- --storage-class=shiftpv-retain",
	} {
		if lines[flag] != 1 {
			t.Fatalf("wanted one lifecycle flag %q", flag)
		}
	}
}
