package helm

import (
	"slices"
	"testing"
)

func TestNodeMoveCleanupRecoveryRightsDoNotDependOnRPC(t *testing.T) {
	for _, enabled := range []string{"true", "false"} {
		t.Run(enabled, func(t *testing.T) {
			output, err := render(t, "--set", "nodeRPC.enabled="+enabled)
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			role := parseChart(t, output).wantObject(t, "Node Move journal recovery", "ClusterRole", fullname+"-node")
			for resource, verbs := range map[string][]string{"shiftpvmoves": {"get", "list", "watch"}, "shiftpvmoves/status": {"update"}} {
				found := false
				for _, rule := range role.Rules {
					if !slices.Contains(rule.Resources, resource) {
						continue
					}
					found = true
					if !slices.Equal(rule.Verbs, verbs) {
						t.Fatalf("%s has unexpected verbs: %v", resource, rule.Verbs)
					}
				}
				if !found {
					t.Fatalf("missing %s permissions", resource)
				}
			}
		})
	}
}
