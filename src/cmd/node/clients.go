package main

import (
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"k8s.io/client-go/rest"
)

type nodeAPIConfigs struct {
	foreground, observation, measurement, metadata, effects *rest.Config
}

type nodeAPIClients struct {
	foreground, observation, measurement, metadata *volumeapi.Registry
	effects                                        wiring.Clients
}

// Each role owns a bounded budget, even if the base config has a limiter.
func newNodeAPIConfigs(base *rest.Config, observe func(string, time.Duration)) nodeAPIConfigs {
	role := func(name string, qps float32, burst int) *rest.Config {
		config := rest.CopyConfig(base)
		config.QPS, config.Burst = qps, burst
		config.RateLimiter = wiring.NewAPIBudget(qps, burst)
		config.UserAgent = "shiftpv-node-" + name
		return wiring.WithAPIObservation(config, "node_api_"+name, observe)
	}
	return nodeAPIConfigs{
		foreground:  role("foreground", 50, 100),
		observation: role("observation", 5, 10),
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
		observation: registry(configs.observation, "observation"),
		measurement: registry(configs.measurement, "measurement"),
		metadata:    registry(configs.metadata, "metadata"),
		effects:     wiring.ForConfig(configs.effects, "resident Node effects", fail),
	}
}
