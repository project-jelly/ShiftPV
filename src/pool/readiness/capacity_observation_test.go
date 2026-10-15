package readiness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func TestCapacityObservationReusesReadinessProbeAndRecovers(t *testing.T) {
	var inspectErr, writeErr, statErr error
	var filesystem poolcapacity.Filesystem
	statCalls := 0
	deps := HostProbeDependencies("/host")
	deps.InspectDirectory = func(string) error { return inspectErr }
	deps.Write = func(string) error { return writeErr }
	deps.StatFS = func(string) (poolcapacity.Filesystem, error) {
		statCalls++
		return filesystem, statErr
	}
	probe, err := NewProbeWithDependencies("/host", deps)
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeRepository{pool: testRegistration(volumeapi.Pool{
		Name: "pool", UID: "pool-uid", NodeName: "node", Generation: 1,
	})}
	now := testTime
	r, err := NewReconciler("node", repository, probe, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return now }
	r.Inventory = func(_ context.Context, _ volumeapi.Pool, observed time.Time) volumeapi.PoolInventory {
		return volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(observed)}
	}
	for _, step := range []struct {
		name       string
		filesystem poolcapacity.Filesystem
		inspectErr error
		writeErr   error
		statErr    error
		ready      bool
	}{
		{name: "available", filesystem: poolcapacity.Filesystem{TotalBytes: 1024, AvailableBytes: 128, AvailableInodes: 16}, ready: true},
		{name: "statfs failure", filesystem: poolcapacity.Filesystem{TotalBytes: 1024, AvailableBytes: 512, AvailableInodes: 32}, statErr: errors.New("statfs unavailable")},
		{name: "recovered", filesystem: poolcapacity.Filesystem{TotalBytes: 1024, AvailableBytes: 256, AvailableInodes: 8}, ready: true},
		{name: "zero available bytes", filesystem: poolcapacity.Filesystem{TotalBytes: 1024, AvailableInodes: 8}, ready: true},
		{name: "zero available inodes", filesystem: poolcapacity.Filesystem{TotalBytes: 1024, AvailableBytes: 256}, ready: true},
		{name: "write failure with readable capacity", filesystem: poolcapacity.Filesystem{TotalBytes: 1024, AvailableBytes: 128, AvailableInodes: 16}, writeErr: syscall.ENOSPC},
		{name: "inaccessible skips statfs", inspectErr: os.ErrNotExist},
		{name: "recovered after skipped probe", filesystem: poolcapacity.Filesystem{TotalBytes: 2048, AvailableBytes: 768, AvailableInodes: 64}, ready: true},
	} {
		t.Run(step.name, func(t *testing.T) {
			filesystem, inspectErr, writeErr, statErr = step.filesystem, step.inspectErr, step.writeErr, step.statErr
			before := statCalls
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if inspectErr != nil {
				wantCalls = 0
			}
			if statCalls-before != wantCalls {
				t.Fatalf("capacity publication repeated statfs: calls=%d want=%d", statCalls-before, wantCalls)
			}
			status := repository.status
			if status.ObservedGeneration != repository.pool.Generation || !status.LastProbeTime.Equal(&metav1.Time{Time: now}) {
				t.Fatalf("capacity escaped readiness observation: %+v", status)
			}
			if inspectErr != nil || statErr != nil {
				if status.FilesystemTotalBytes != 0 || status.FilesystemAvailableBytes != nil || status.FilesystemAvailableInodes != nil {
					t.Fatalf("failed probe retained capacity: %+v", status)
				}
			} else if status.FilesystemTotalBytes != filesystem.TotalBytes || status.FilesystemAvailableBytes == nil ||
				*status.FilesystemAvailableBytes != filesystem.AvailableBytes || status.FilesystemAvailableInodes == nil ||
				*status.FilesystemAvailableInodes != filesystem.AvailableInodes {
				t.Fatalf("successful probe lost capacity, including zero: %+v", status)
			}
			repository.pool.Status = status
			if ready, reason := repository.pool.ReadyAt(now, time.Minute); ready != step.ready {
				t.Fatalf("capacity observation changed readiness: ready=%v reason=%s want=%v", ready, reason, step.ready)
			}
			repository.pool.Generation++
			now = now.Add(time.Minute)
		})
	}
}

func TestCapacityObservationSerializationDistinguishesMissingAndZero(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bytes  *int64
		inodes *int64
	}{
		{name: "older node"},
		{name: "zero bytes", bytes: ptr.To[int64](0), inodes: ptr.To[int64](8)},
		{name: "zero inodes", bytes: ptr.To[int64](128), inodes: ptr.To[int64](0)},
		{name: "available", bytes: ptr.To[int64](256), inodes: ptr.To[int64](16)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := volumeapi.PoolStatus{FilesystemTotalBytes: 1024, FilesystemAvailableBytes: tc.bytes,
				FilesystemAvailableInodes: tc.inodes, ObservedGeneration: 3, LastProbeTime: metav1.NewTime(testTime)}
			data, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			var encoded map[string]json.RawMessage
			if err := json.Unmarshal(data, &encoded); err != nil {
				t.Fatal(err)
			}
			for field, present := range map[string]bool{"filesystemAvailableBytes": tc.bytes != nil, "filesystemAvailableInodes": tc.inodes != nil} {
				if _, found := encoded[field]; found != present {
					t.Fatalf("JSON field %s presence=%v want=%v: %s", field, found, present, data)
				}
			}
			var jsonStatus volumeapi.PoolStatus
			if err := json.Unmarshal(data, &jsonStatus); err != nil || !apiequality.Semantic.DeepEqual(jsonStatus, want) {
				t.Fatalf("JSON round trip lost observation: %+v err=%v", jsonStatus, err)
			}
			unstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&want)
			if err != nil {
				t.Fatal(err)
			}
			var apiStatus volumeapi.PoolStatus
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructured, &apiStatus); err != nil || !apiequality.Semantic.DeepEqual(apiStatus, want) {
				t.Fatalf("API round trip lost observation: %+v err=%v", apiStatus, err)
			}
		})
	}
}
