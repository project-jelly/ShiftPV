package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestOperationTimeoutFlag(t *testing.T) {
	if os.Getenv("SHIFTPV_TEST_OPERATION_TIMEOUT") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"controller"}, os.Args[i+1:]...)
				break
			}
		}
		flag.CommandLine = flag.NewFlagSet("controller", flag.ExitOnError)
		cfg := parseFlags()
		fmt.Println("operation-timeout=" + cfg.mobilityOperationTimeout.String())
		return
	}
	for _, test := range []struct {
		name, value, want string
		fail              bool
	}{
		{"default", "", "operation-timeout=5m0s", false},
		{"configured", "30m", "operation-timeout=30m0s", false},
		{"zero", "0s", "must be positive", true},
		{"negative", "-1m", "must be positive", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []string{"-test.run=^TestOperationTimeoutFlag$", "--"}
			if test.value != "" {
				args = append(args, "--mobility-operation-timeout="+test.value)
			}
			command := exec.Command(os.Args[0], args...)
			command.Env = append(os.Environ(), "SHIFTPV_TEST_OPERATION_TIMEOUT=1")
			output, err := command.CombinedOutput()
			if (err != nil) != test.fail || !strings.Contains(string(output), test.want) {
				t.Fatalf("operation timeout flag: error=%v output=%s", err, output)
			}
		})
	}
}
