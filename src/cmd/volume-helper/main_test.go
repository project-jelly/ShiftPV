package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// TestHelperForwardsPoolReadinessBudgetToRegistry pins that every node-bound
// helper subcommand accepts --pool-readiness-stale-after and carries it into
// its Registry. The cleanup subcommand is the one that consumes it: its move
// source publication proof calls ReadyPoolForNode, so a dropped flag would
// silently fall back to DefaultPoolReadinessStaleAfter and accept a Pool probe
// the parent controller already treats as stale. The move subcommands recheck
// authority through PoolForNode and hold the value only so a controller may
// forward it without version-skewing the CLI.
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

	cleanupIdentity := []string{
		"--authority-kind=ShiftPVMove", "--authority-name=move-test", "--authority-uid=move-uid",
		"--operation-id=cleanup-move-uid", "--namespace=shiftpv-system",
	}
	cleanupDefaults, err := parseCleanupOptions(cleanupIdentity)
	if err != nil {
		t.Fatalf("cleanup default parse: %v", err)
	}
	if cleanupDefaults.poolReadinessStaleAfter != volumeapi.DefaultPoolReadinessStaleAfter {
		t.Fatalf("cleanup default budget = %v", cleanupDefaults.poolReadinessStaleAfter)
	}
	cleanup, err := parseCleanupOptions(append(append([]string{}, cleanupIdentity...),
		volumeapi.PoolReadinessStaleAfterArgument(budget)))
	if err != nil {
		t.Fatalf("cleanup forwarded parse: %v", err)
	}
	if cleanup.poolReadinessStaleAfter != budget {
		t.Fatalf("cleanup parsed budget = %v", cleanup.poolReadinessStaleAfter)
	}
	registry := &volumeapi.Registry{PoolReadinessStaleAfter: cleanup.poolReadinessStaleAfter}
	if registry.PoolReadinessStaleAfter != budget {
		t.Fatalf("cleanup Registry budget = %v", registry.PoolReadinessStaleAfter)
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
