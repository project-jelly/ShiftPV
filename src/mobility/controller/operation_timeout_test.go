package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperationTimeoutBoundsNewJobs(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		want    int64
	}{
		{"default", 0, 300},
		{"large copy", 30 * time.Minute, 1800},
		{"round up", 1500 * time.Millisecond, 2},
		{"subsecond", time.Millisecond, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := newMobilityWalk()
			w.observeAndLock(t)
			w.ensurePlacementOnDestination(t)
			w.reconciler.OperationTimeout = test.timeout
			for _, ensure := range []func() error{
				func() error { return w.reconciler.ensureCopyJob(w.ctx, w.move, w.observed.Names) },
				func() error { return w.reconciler.ensurePromotionJob(w.ctx, w.move, w.observed.Names) },
			} {
				if err := ensure(); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{w.observed.Names.CopyJob, w.observed.Names.PromotionJob} {
				job, err := w.client.BatchV1().Jobs("system").Get(w.ctx, name, metav1.GetOptions{})
				if err != nil || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != test.want {
					t.Fatalf("new operation has the wrong deadline: job=%+v error=%v", job, err)
				}
			}
		})
	}
}

func TestOperationTimeoutChangeRetainsExistingJob(t *testing.T) {
	w := newMobilityWalk()
	w.observeAndLock(t)
	w.ensurePlacementOnDestination(t)
	if err := w.reconciler.ensureCopyJob(w.ctx, w.move, w.observed.Names); err != nil {
		t.Fatal(err)
	}
	before, err := w.client.BatchV1().Jobs("system").Get(w.ctx, w.observed.Names.CopyJob, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	w.reconciler.OperationTimeout = 30 * time.Minute
	if err := w.reconciler.ensureCopyJob(w.ctx, w.move, w.observed.Names); err != nil {
		t.Fatalf("configuration change invalidated the approved Job: %v", err)
	}
	after, err := w.client.BatchV1().Jobs("system").Get(w.ctx, before.Name, metav1.GetOptions{})
	if err != nil || after.UID != before.UID || *after.Spec.ActiveDeadlineSeconds != 300 {
		t.Fatalf("existing Job was replaced or changed: job=%+v error=%v", after, err)
	}
	if err := w.reconciler.ensurePromotionJob(w.ctx, w.move, w.observed.Names); err != nil {
		t.Fatal(err)
	}
	newJob, err := w.client.BatchV1().Jobs("system").Get(w.ctx, w.observed.Names.PromotionJob, metav1.GetOptions{})
	if err != nil || *newJob.Spec.ActiveDeadlineSeconds != 1800 {
		t.Fatalf("new Job did not use changed configuration: job=%+v error=%v", newJob, err)
	}
}

func TestOperationTimeoutRejectsUnboundedExistingCopy(t *testing.T) {
	zero := int64(0)
	for _, deadline := range []*int64{nil, &zero} {
		w := newMobilityWalk()
		w.observeAndLock(t)
		w.ensurePlacementOnDestination(t)
		if err := w.reconciler.ensureCopyJob(w.ctx, w.move, w.observed.Names); err != nil {
			t.Fatal(err)
		}
		job, err := w.client.BatchV1().Jobs("system").Get(w.ctx, w.observed.Names.CopyJob, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		job.Spec.ActiveDeadlineSeconds = deadline
		if _, err := w.client.BatchV1().Jobs("system").Update(w.ctx, job, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := w.reconciler.ensureCopyJob(w.ctx, w.move, w.observed.Names); err == nil {
			t.Fatal("unbounded existing copy was accepted")
		}
	}
}

func TestNegativeOperationTimeoutCreatesNoJob(t *testing.T) {
	w := newMobilityWalk()
	w.observeAndLock(t)
	w.ensurePlacementOnDestination(t)
	w.reconciler.OperationTimeout = -time.Second
	if err := w.reconciler.ensureCopyJob(w.ctx, w.move, w.observed.Names); err == nil {
		t.Fatal("negative timeout was accepted")
	}
	jobs, err := w.client.BatchV1().Jobs("system").List(w.ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("invalid timeout acquired a Job: jobs=%+v error=%v", jobs, err)
	}
}
