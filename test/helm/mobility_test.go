package helm

import (
	"strings"
	"testing"
)

func TestMoveJournalRetention(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantFlag   string
		wantSchema bool
	}{
		{name: "default", wantFlag: "--move-journal-retention=168h"},
		{name: "one hour", args: []string{"--set", "mobility.journalRetention=1h"}, wantFlag: "--move-journal-retention=1h"},
		{name: "zero", args: []string{"--set", "mobility.journalRetention=0h"}, wantSchema: true},
		{name: "minutes", args: []string{"--set", "mobility.journalRetention=30m"}, wantSchema: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := render(t, tc.args...)
			if tc.wantSchema {
				if err == nil || !strings.Contains(output, "schema") {
					t.Fatalf("wanted schema failure: %v %s", err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			if strings.Count(output, tc.wantFlag) != 1 {
				t.Fatalf("wanted exactly one %q flag", tc.wantFlag)
			}
		})
	}
}

func TestMobilityPlacementPriorityClass(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		args         []string
		disabled     bool
	}{
		{name: "default", prefix: fullname},
		{name: "renamed release", prefix: "custom", args: []string{"--set", "fullnameOverride=custom"}},
		{name: "disabled", prefix: fullname, args: []string{"--set", "mobility.enabled=false"}, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := render(t, tc.args...)
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			chart := parseChart(t, output)
			controller := chart.container(t, "Deployment", tc.prefix+"-controller", "shiftpv-controller")
			args := controller.Spec.Template.Spec.Containers[0].Args
			if tc.disabled {
				chart.wantNoObject(t, "disabled mobility creates no placement class", "PriorityClass", "")
				wantNoArgPrefix(t, "disabled mobility has no placement class flag", args, "--mobility-placement-priority-class=")
				return
			}
			name := tc.prefix + "-placement"
			class := chart.wantObject(t, "dedicated non-preempting placement", "PriorityClass", name)
			if class.Value == nil || *class.Value != 0 || class.GlobalDefault == nil || *class.GlobalDefault || class.PreemptionPolicy != "Never" || class.Metadata.Namespace != "" {
				t.Fatalf("unsafe placement PriorityClass: %+v", class)
			}
			wantArg(t, "controller uses the installed class", args, "--mobility-placement-priority-class="+name)
		})
	}
}
