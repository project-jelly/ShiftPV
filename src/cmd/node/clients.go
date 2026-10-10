package main

import (
	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

type nodeAPIConfigs struct {
	foreground, measurement, metadata, effects *rest.Config
}

type nodeAPIClients struct {
	foreground, measurement, metadata *volumeapi.Registry
	effects                           wiring.Clients
}

// Each role owns a bounded budget, even if the base config has a limiter.
func newNodeAPIConfigs(base *rest.Config) nodeAPIConfigs {
	role := func(name string, qps float32, burst int) *rest.Config {
		config := rest.CopyConfig(base)
		config.QPS, config.Burst = qps, burst
		config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(qps, burst)
		config.UserAgent = "shiftpv-node-" + name
		return config
	}
	return nodeAPIConfigs{
		foreground:  role("foreground", 5, 10),
		measurement: role("measurement", 5, 10),
		metadata:    role("metadata", 5, 10),
		effects:     role("effects", 50, 100),
	}
}

func newNodeAPIClients(configs nodeAPIConfigs, fail wiring.Fail) nodeAPIClients {
	registry := func(config *rest.Config, role string) *volumeapi.Registry {
		return &volumeapi.Registry{Client: wiring.DynamicForConfig(config, role, fail)}
	}
	return nodeAPIClients{
		foreground:  registry(configs.foreground, "foreground"),
		measurement: registry(configs.measurement, "measurement"),
		metadata:    registry(configs.metadata, "metadata"),
		effects:     wiring.ForConfig(configs.effects, "resident Node effects", fail),
	}
}
