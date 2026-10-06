package main

import (
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestCapacityRetryUsesIndependentRateBudget(t *testing.T) {
	original := &rest.Config{QPS: 100, Burst: 100, RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), Timeout: time.Minute, UserAgent: "controller"}
	retry := capacityRetryRESTConfig(original)
	if retry.RateLimiter == original.RateLimiter || retry.QPS != 5 || retry.Burst != 10 || retry.Timeout != 0 {
		t.Fatal("retry observation shares the CSI budget")
	}
	if original.QPS != 100 || original.Burst != 100 || original.Timeout != time.Minute || original.UserAgent != "controller" {
		t.Fatal("original REST config changed")
	}
	original.RateLimiter.TryAccept()
	if !retry.RateLimiter.TryAccept() {
		t.Fatal("CSI rate limiting leaked into notification")
	}
}
