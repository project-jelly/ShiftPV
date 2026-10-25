package main

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestHelperForwardsPoolReadinessBudgetToRegistry(t *testing.T) {
	t.Setenv("POD_NAME", "helper-pod")
	const budget = 7 * time.Minute

	moveIdentity := []string{
		"--move-name=move-test", "--move-uid=move-uid",
		"--operation-id=copy-move-uid", "--namespace=shiftpv-system",
	}
	copyIdentity := append(append([]string{}, moveIdentity...),
		"--source-service=move-test-source", "--password-file=/auth/password")
	for action, identity := range map[string][]string{
		"serve-source": moveIdentity, "promote": moveIdentity, "verify-owner": moveIdentity, "copy": copyIdentity,
	} {
		copyAction := action == "copy"
		defaults, err := parseMoveOptions(action, identity, copyAction)
		if err != nil {
			t.Fatalf("%s default parse: %v", action, err)
		}
		if defaults.poolReadinessStaleAfter != volumeapi.DefaultPoolReadinessStaleAfter {
			t.Fatalf("%s default budget = %v", action, defaults.poolReadinessStaleAfter)
		}
		forwarded := append(append([]string{}, identity...), volumeapi.PoolReadinessStaleAfterArgument(budget))
		options, err := parseMoveOptions(action, forwarded, copyAction)
		if err != nil {
			t.Fatalf("%s forwarded parse: %v", action, err)
		}
		if options.poolReadinessStaleAfter != budget {
			t.Fatalf("%s parsed budget = %v", action, options.poolReadinessStaleAfter)
		}
		registry := &volumeapi.Registry{PoolReadinessStaleAfter: options.poolReadinessStaleAfter}
		if registry.PoolReadinessStaleAfter != budget {
			t.Fatalf("%s Registry budget = %v", action, registry.PoolReadinessStaleAfter)
		}
	}

}

func TestMoveCopyArgumentsPreserveFilesystemContract(t *testing.T) {
	copyArguments, verifyArguments := moveCopyArguments("rsync://source/data/", "/pool/incoming")
	wantCopy := []string{
		"-aHAXS", "--numeric-ids", "--one-file-system", "--no-devices", "--delete", "--fsync",
		"--compress", "--compress-choice=zstd", "--compress-level=1",
		"rsync://source/data/", "/pool/incoming/",
	}
	wantVerify := []string{
		"-aHAXS", "--numeric-ids", "--one-file-system", "--no-devices", "--delete",
		"--checksum", "--dry-run", "--itemize-changes", "rsync://source/data/", "/pool/incoming/",
	}
	if !reflect.DeepEqual(copyArguments, wantCopy) {
		t.Fatalf("copy arguments = %#v", copyArguments)
	}
	if !reflect.DeepEqual(verifyArguments, wantVerify) {
		t.Fatalf("verify arguments = %#v", verifyArguments)
	}
}

func TestRetiredEffectsAreNotHelperCommands(t *testing.T) {
	previous := os.Args
	t.Cleanup(func() { os.Args = previous })
	for _, action := range []string{"create", "cleanup"} {
		os.Args = []string{"shiftpv-volume-helper", action}
		if err := run(); err == nil || !strings.Contains(err.Error(), "unknown volume helper action") {
			t.Fatalf("retired effect reached infrastructure: %s %v", action, err)
		}
	}
}
