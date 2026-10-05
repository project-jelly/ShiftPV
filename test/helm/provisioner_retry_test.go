package helm

import (
	"strings"
	"testing"
)

func TestProvisionerRetryIntervals(t *testing.T) {
	for _, test := range []struct {
		name       string
		values     []string
		start, max string
		invalid    bool
	}{
		{name: "default", start: "1s", max: "30s"},
		{name: "custom", values: []string{"--set", "sidecars.provisioner.retryIntervalStart=2s,sidecars.provisioner.retryIntervalMax=5m"}, start: "2s", max: "5m"},
		{name: "zero", values: []string{"--set", "sidecars.provisioner.retryIntervalMax=0s"}, invalid: true},
		{name: "malformed", values: []string{"--set", "sidecars.provisioner.retryIntervalStart=soon"}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := render(t, test.values...)
			if test.invalid {
				if err == nil {
					t.Fatal("invalid retry interval accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			for _, flag := range []string{"--retry-interval-start=" + test.start, "--retry-interval-max=" + test.max} {
				if !strings.Contains(output, flag) {
					t.Fatalf("missing %s", flag)
				}
			}
		})
	}
}
