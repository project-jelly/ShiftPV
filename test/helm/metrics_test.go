package helm

import (
	"os/exec"
	"strings"
	"testing"
)

func render(t *testing.T, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", append([]string{"template", "test", "../../charts/shiftpv", "--namespace", "storage", "--kube-version", "1.35.8"}, args...)...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestMetricsValues(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		args                      []string
		failure                   string
		services, monitors, rules int
	}{
		{name: "default"},
		{name: "enabled", args: []string{"--set", "metrics.enabled=true"}, services: 2},
		{name: "monitor", args: []string{"--set", "metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.serviceMonitor.additionalLabels.release=kube-prometheus-stack", "--api-versions", "monitoring.coreos.com/v1/ServiceMonitor"}, services: 2, monitors: 1},
		{name: "alerts", args: []string{"--set", "metrics.enabled=true,metrics.prometheusRule.enabled=true,metrics.prometheusRule.additionalLabels.release=kube-prometheus-stack", "--api-versions", "monitoring.coreos.com/v1/PrometheusRule"}, services: 2, rules: 1},
		{name: "monitor without metrics", args: []string{"--set", "metrics.serviceMonitor.enabled=true"}, failure: "requires metrics.enabled"},
		{name: "alerts without metrics", args: []string{"--set", "metrics.prometheusRule.enabled=true", "--api-versions", "monitoring.coreos.com/v1/PrometheusRule"}, failure: "requires metrics.enabled"},
		{name: "missing CRD", args: []string{"--set", "metrics.enabled=true,metrics.serviceMonitor.enabled=true"}, failure: "requires the Prometheus Operator"},
		{name: "missing alert CRD", args: []string{"--set", "metrics.enabled=true,metrics.prometheusRule.enabled=true"}, failure: "requires the Prometheus Operator"},
		{name: "webhook collision", args: []string{"--set", "metrics.enabled=true,metrics.port=9443"}, failure: "must differ"},
		{name: "health collision", args: []string{"--set", "metrics.port=9808"}, failure: "schema"},
		{name: "invalid interval", args: []string{"--set", "metrics.snapshotInterval=0s"}, failure: "schema"},
		{name: "unknown setting", args: []string{"--set", "metrics.enabeld=true"}, failure: "schema"},
		{name: "timeout", args: []string{"--set", "metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.serviceMonitor.scrapeTimeout=40s", "--api-versions", "monitoring.coreos.com/v1/ServiceMonitor"}, failure: "must not exceed"},
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
			if strings.Count(output, "targetPort: metrics") != tc.services || strings.Count(output, "kind: ServiceMonitor") != tc.monitors || strings.Count(output, "kind: PrometheusRule") != tc.rules {
				t.Fatal("unexpected service/monitor/rule count")
			}
			if tc.services == 0 && strings.Contains(output, "--metrics-") {
				t.Fatal("disabled metrics has flags")
			}
			if tc.services == 2 && strings.Count(output, "--metrics-listen-address=:8080") != 2 {
				t.Fatal("missing listener wiring")
			}
			if tc.monitors == 1 {
				for _, want := range []string{"release: kube-prometheus-stack", "matchNames: [\"storage\"]", "shiftpv.io/metrics: \"true\"", "port: metrics", "targetLabel: component", "targetLabel: node", "targetLabel: release"} {
					if !strings.Contains(output, want) {
						t.Fatalf("missing monitor contract %q", want)
					}
				}
			}
			if tc.rules == 1 {
				for _, want := range []string{"release: kube-prometheus-stack", "ShiftPVObservationFailed", "ShiftPVObservationStale", "ShiftPVPoolAccountingInvalid", "ShiftPVPoolInventoryUnsafe", "ShiftPVCleanupNeedsReview", "ShiftPVCopyNeedsReview", "ShiftPVReleasedVolumes", "ShiftPVPoolNoSpace", "ShiftPVVolumesWithoutPersistentVolume", "ShiftPVMoveBlocked", "ShiftPVPoolNotReady", "ShiftPVCSIErrorRate"} {
					if !strings.Contains(output, want) {
						t.Fatalf("missing alert contract %q", want)
					}
				}
			}
			again, err := render(t, tc.args...)
			if err != nil || again != output {
				t.Fatal("nondeterministic template")
			}
		})
	}
}
