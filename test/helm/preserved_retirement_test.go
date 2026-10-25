package helm

import "testing"

func TestUninstallPreservedRetirementIsOptInAndUsesExactUIDFlags(t *testing.T) {
	output, err := render(t)
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	guard := parseChart(t, output).guardContainer(t).Spec.Template.Spec.Containers[0]
	wantNoArgPrefix(t, "default retains fail closed uninstall", guard.Args, "--preserve-retired-pool=")
	output, err = render(t, "--set-json", `lifecycle.preserveRetiredPoolUIDs=["pool-a-uid","pool-b-uid"]`)
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	chart := parseChart(t, output)
	guard = chart.guardContainer(t).Spec.Template.Spec.Containers[0]
	wantArg(t, "first exact UID", guard.Args, "--preserve-retired-pool=pool-a-uid")
	wantArg(t, "second exact UID", guard.Args, "--preserve-retired-pool=pool-b-uid")
	wantNoArgPrefix(t, "preservation config affects only uninstall guard", chart.controllerArgs(t), "--preserve-retired-pool=")
}

func TestUninstallPreservedRetirementRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{`"pool-a-uid"`, `[""]`, `["pool-a-uid","pool-a-uid"]`, `[true]`} {
		t.Run(value, func(t *testing.T) {
			if output, err := render(t, "--set-json", "lifecycle.preserveRetiredPoolUIDs="+value); err == nil {
				t.Fatalf("invalid preservation accepted: %s", output)
			}
		})
	}
}
