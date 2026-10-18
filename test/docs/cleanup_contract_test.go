package docs_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
)

func TestCRDsUseKubernetesStructuralObjectSchemas(t *testing.T) {
	root := repositoryRoot(t)
	paths, err := filepath.Glob(filepath.Join(root, "charts", "shiftpv", "crds", "*.yaml"))
	if err != nil {
		t.Fatalf("list CRDs: %v", err)
	}
	wantFiles := []string{"shiftpv.io_shiftpvmoves.yaml", "shiftpv.io_shiftpvpools.yaml", "shiftpv.io_shiftpvvolumes.yaml"}
	gotFiles := make([]string, 0, len(paths))
	for _, path := range paths {
		gotFiles = append(gotFiles, filepath.Base(path))
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if bytes.Contains(data, []byte("additionalProperties: false")) {
			t.Errorf("%s uses a JSON Schema closure rejected by the Kubernetes CRD structural schema", filepath.Base(path))
		}
	}
	sort.Strings(gotFiles)
	if !reflect.DeepEqual(gotFiles, wantFiles) {
		t.Fatalf("installed CRDs=%v want exactly %v", gotFiles, wantFiles)
	}
}

func TestVolumeInitialPoolSchemaPreservesLegacyObjects(t *testing.T) {
	crd := readYAMLMap(t, filepath.Join(repositoryRoot(t), "charts", "shiftpv", "crds", "shiftpv.io_shiftpvvolumes.yaml"))
	spec := requiredMap(t, singleCRDVersion(t, crd), "schema", "openAPIV3Schema", "properties", "spec")
	for _, field := range requiredStrings(t, spec, "required") {
		if field == "initialPool" {
			t.Fatal("old Volumes must remain valid without initialPool")
		}
	}
	initialPool := requiredMap(t, spec, "properties", "initialPool")
	assertStringSet(t, requiredStrings(t, initialPool, "required"), []string{"name", "uid"}, "initial reservation identity")
	rules, _, _ := unstructured.NestedSlice(spec, "x-kubernetes-validations")
	want := "!has(oldSelf.initialPool) || (has(self.initialPool) && self.initialPool == oldSelf.initialPool)"
	for _, value := range rules {
		if rule, ok := value.(map[string]any); ok && rule["rule"] == want {
			return
		}
	}
	t.Fatal("initialPool must support one-time legacy binding and reject subsequent removal or change")
}

func TestParentCleanupJournalSchemasMatchRuntimeContract(t *testing.T) {
	root := repositoryRoot(t)
	wantPhases := []string{
		cleanupapi.PhasePending,
		cleanupapi.PhaseRunning,
		cleanupapi.PhaseVerifying,
		cleanupapi.PhaseConfirmingAbsence,
		cleanupapi.PhaseCompleted,
		cleanupapi.PhaseNeedsReview,
	}
	for _, tc := range []struct {
		file, parentKind string
		reasons          []string
	}{
		{"shiftpv.io_shiftpvvolumes.yaml", "ShiftPVVolume", []string{"VolumeDelete", "CreationCancelled"}},
		{"shiftpv.io_shiftpvmoves.yaml", "ShiftPVMove", []string{"MoveSource", "MoveRollback"}},
	} {
		t.Run(tc.parentKind, func(t *testing.T) {
			crd := readYAMLMap(t, filepath.Join(root, "charts", "shiftpv", "crds", tc.file))
			version := singleCRDVersion(t, crd)
			cleanup := requiredMap(t, version, "schema", "openAPIV3Schema", "properties", "status", "properties", "cleanup")
			if cleanup["type"] != "object" {
				t.Fatalf("status.cleanup type=%v want object", cleanup["type"])
			}
			assertStringSet(t, requiredStrings(t, cleanup, "required"), []string{"spec", "status"}, "cleanup required fields")

			spec := requiredMap(t, cleanup, "properties", "spec")
			assertStringSet(t, requiredStrings(t, spec, "required"), []string{"operationID", "target", "reason", "authority"}, "cleanup spec required fields")
			specProperties := requiredMap(t, spec, "properties")
			for _, forbidden := range []string{"approved", "path", "reservationUID"} {
				if _, found := specProperties[forbidden]; found {
					t.Fatalf("embedded cleanup spec exposes removed field %q", forbidden)
				}
			}
			authorityKinds := requiredStrings(t, spec, "properties", "authority", "properties", "kind", "enum")
			if !reflect.DeepEqual(authorityKinds, []string{tc.parentKind}) {
				t.Fatalf("cleanup authority kinds=%v want [%s]", authorityKinds, tc.parentKind)
			}
			reasons := requiredStrings(t, spec, "properties", "reason", "enum")
			if !reflect.DeepEqual(reasons, tc.reasons) {
				t.Fatalf("cleanup reasons=%v want %v", reasons, tc.reasons)
			}

			status := requiredMap(t, cleanup, "properties", "status")
			phases := requiredStrings(t, status, "properties", "phase", "enum")
			if !reflect.DeepEqual(phases, wantPhases) {
				t.Fatalf("cleanup phases=%v want=%v", phases, wantPhases)
			}
			receipt := requiredMap(t, status, "properties", "receipt")
			assertStringSet(t, requiredStrings(t, receipt, "required"), []string{"operationID", "executorUID", "observedAt", "retired", "purged", "localReceiptDigest"}, "cleanup receipt required fields")
			if pattern, _, _ := unstructured.NestedString(receipt, "properties", "localReceiptDigest", "pattern"); pattern != "^[0-9a-f]{64}$" {
				t.Fatalf("cleanup receipt digest pattern=%q", pattern)
			}
			absence := requiredMap(t, status, "properties", "absenceProof")
			assertStringSet(t, requiredStrings(t, absence, "required"), []string{"requestID", "poolName", "poolUID", "requiredGeneration"}, "cleanup absence proof required fields")
			absenceProperties := requiredMap(t, absence, "properties")
			for _, field := range []string{"observedGeneration", "valid", "complete", "absent", "confirmedAt"} {
				if _, found := absenceProperties[field]; !found {
					t.Errorf("cleanup absence proof is missing %q", field)
				}
			}
		})
	}
}

func readYAMLMap(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	return crd
}

func singleCRDVersion(t *testing.T, crd map[string]any) map[string]any {
	t.Helper()
	versions, found, err := unstructured.NestedSlice(crd, "spec", "versions")
	if err != nil || !found || len(versions) != 1 {
		t.Fatalf("CRD versions=%d found=%t err=%v", len(versions), found, err)
	}
	version, ok := versions[0].(map[string]any)
	if !ok {
		t.Fatalf("CRD version has type %T", versions[0])
	}
	return version
}

func requiredMap(t *testing.T, object map[string]any, fields ...string) map[string]any {
	t.Helper()
	value, found, err := unstructured.NestedMap(object, fields...)
	if err != nil || !found {
		t.Fatalf("required map %v found=%t err=%v", fields, found, err)
	}
	return value
}

func requiredStrings(t *testing.T, object map[string]any, fields ...string) []string {
	t.Helper()
	values, found, err := unstructured.NestedStringSlice(object, fields...)
	if err != nil || !found {
		t.Fatalf("required string list %v found=%t err=%v", fields, found, err)
	}
	return values
}

func assertStringSet(t *testing.T, got, want []string, label string) {
	t.Helper()
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s=%v want=%v", label, got, want)
	}
}
