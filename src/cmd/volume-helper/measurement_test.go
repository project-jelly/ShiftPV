package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type measurementRepository struct {
	pool  volumeapi.Pool
	state volumeapi.State
	err   error
}

func (r *measurementRepository) ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return r.pool, r.err
}
func (r *measurementRepository) Get(context.Context, string) (volumeapi.State, error) {
	return r.state, r.err
}
func (r *measurementRepository) InstallationID(context.Context) (string, error) {
	return "installation", r.err
}

func measurementFixture(t *testing.T) (*measurementRepository, measurementOptions) {
	t.Helper()
	pool := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/mnt/a", Generation: 1}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: pool.Name, PoolUID: pool.UID, NodeName: pool.NodeName,
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", Role: volume.RoleServing}
	evidence, err := capacity.MeasurementEvidence(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &measurementRepository{pool: pool, state: volumeapi.State{UID: copy.VolumeUID, OwnerNode: copy.NodeName, Phase: volumeapi.PhaseMoving, CurrentCopy: &copy}},
		measurementOptions{pool: pool, root: t.TempDir(), evidence: evidence, volumeID: copy.VolumeID, copy: &copy}
}

func TestMeasurementRejectsContradictionsBeforeReading(t *testing.T) {
	for _, scenario := range []string{"API error", "generation", "mount", "copy marker", "copy identity", "published", "installation"} {
		t.Run(scenario, func(t *testing.T) {
			repo, options := measurementFixture(t)
			verifyMount := func(string, volumeapi.Pool) error { return nil }
			verifyCopy := func(string, volume.CopyIdentity) error { return nil }
			switch scenario {
			case "API error":
				repo.err = errors.New("API unavailable")
			case "generation":
				repo.pool.Generation++
			case "mount":
				verifyMount = func(string, volumeapi.Pool) error { return errors.New("mount changed") }
			case "copy marker":
				verifyCopy = func(string, volume.CopyIdentity) error { return errors.New("marker mismatch") }
			case "copy identity":
				copy := *repo.state.CurrentCopy
				copy.CopyID = "replacement-copy"
				repo.state.CurrentCopy = &copy
			case "published":
				repo.state.PublishedNodes = []string{repo.pool.NodeName}
			case "installation":
				copy := *repo.state.CurrentCopy
				copy.InstallationID = "replacement"
				repo.state.CurrentCopy = &copy
				options.copy = nil
			}
			reads := 0
			output, err := measureWithAuthority(context.Background(), repo, options, verifyMount, verifyCopy, func(context.Context) (string, error) { reads++; return "123", nil })
			if err == nil || output != "" || reads != 0 {
				t.Fatalf("output=%q error=%v reads=%d", output, err, reads)
			}
		})
	}
}

func TestMeasurementDiscardsResultWhenAuthorityChangesDuringRead(t *testing.T) {
	for _, scenario := range []string{"Pool", "mount", "copy", "published", "read error"} {
		t.Run(scenario, func(t *testing.T) {
			repo, options := measurementFixture(t)
			options.copy = nil // also exercises pinning for the legacy usage entry point
			mountChecks := 0
			verifyMount := func(string, volumeapi.Pool) error {
				mountChecks++
				if scenario == "mount" && mountChecks == 2 {
					return errors.New("mount disappeared")
				}
				return nil
			}
			output, err := measureWithAuthority(context.Background(), repo, options, verifyMount, func(string, volume.CopyIdentity) error { return nil }, func(context.Context) (string, error) {
				switch scenario {
				case "Pool":
					repo.pool.Generation++
				case "copy":
					copy := *repo.state.CurrentCopy
					copy.CopyID = "replacement-copy"
					repo.state.CurrentCopy = &copy
				case "published":
					repo.state.PublishedNodes = []string{repo.pool.NodeName}
				case "read error":
					return "", errors.New("du failed")
				}
				return "123", nil
			})
			if err == nil || output != "" {
				t.Fatalf("changed authority result accepted: output=%q error=%v", output, err)
			}
		})
	}
}

func TestMeasurementChecksMountAndCopyBeforeAndAfterRead(t *testing.T) {
	repo, options := measurementFixture(t)
	mounts, copies := 0, 0
	output, err := measureWithAuthority(context.Background(), repo, options,
		func(string, volumeapi.Pool) error { mounts++; return nil },
		func(string, volume.CopyIdentity) error { copies++; return nil },
		func(context.Context) (string, error) { return "123", nil })
	if err != nil || output != "123" || mounts != 2 || copies != 2 {
		t.Fatalf("output=%q error=%v mounts=%d copies=%d", output, err, mounts, copies)
	}
}

func TestParseMeasurementRequiresBoundPoolAndCopy(t *testing.T) {
	_, options := measurementFixture(t)
	args := []string{"--pool-name=" + options.pool.Name, "--pool-uid=" + options.pool.UID, "--node-name=" + options.pool.NodeName, "--pool-evidence=" + options.evidence}
	if _, err := parseMeasurementOptions("statfs", args); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMeasurementOptions("statfs", args[:3]); err == nil {
		t.Fatal("missing evidence accepted")
	}
	if _, err := parseMeasurementOptions("usage", append(append([]string{}, args...), "--volume-id=../escape")); err == nil {
		t.Fatal("unsafe volume accepted")
	}
	copy := *options.copy
	copy.PoolUID = "other-uid"
	encoded, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseMeasurementOptions("usage", append(args, "--volume-id="+copy.VolumeID, "--copy-identity="+string(encoded))); err == nil {
		t.Fatal("copy in another Pool accepted")
	}
}

func TestReadMeasurementStatFS(t *testing.T) {
	_, options := measurementFixture(t)
	output, err := readMeasurement(context.Background(), "statfs", options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capacity.ParseStatOutput(output); err != nil {
		t.Fatal(err)
	}
}

func TestReadMeasurementUsagePropagatesDUFailure(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("runtime uses Linux du")
	}
	_, options := measurementFixture(t)
	if _, err := readMeasurement(context.Background(), "usage", options); err == nil {
		t.Fatal("missing serving directory was measured successfully")
	}
	path, err := volume.Path(options.root, options.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "data"), []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := readMeasurement(context.Background(), "usage", options)
	if err != nil || strings.TrimSpace(output) == "" {
		t.Fatalf("output=%q error=%v", output, err)
	}
}
