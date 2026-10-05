package metrics

var volumePhases = []string{"Pending", "NodeCreating", "Ready", "Deleting", "Moving", "Blocked", "Unknown"}
var movePhases = []string{"Pending", "Locking", "Evicting", "WaitingForUnpublish", "WaitingForReplacement", "WaitingForDestination", "WaitingForCapacity", "Copying", "Promoting", "Committing", "ReleasingDestination", "WaitingForDestinationPublish", "CleaningSource", "Completing", "Succeeded", "Blocked", "Unknown"}
var persistentVolumePhases = []string{"Pending", "Available", "Bound", "Released", "Failed", "Unknown"}
var deferredReasons = []string{
	"SourceUnavailable", "SourceNotCordoned", "VolumeBindingMissing", "VolumeBindingMismatch", "OwnerMismatch", "AdmissionNotEnabled",
	"MultipleConsumers", "ControlledConsumerMissing", "BarePodUnsupported", "DestinationUnavailable", "ConsumerUnavailable",
	"MultiplePVCsUnsupported", "ExplicitNodeNameUnsupported", "InvalidPVNodeAffinity", "InvalidNodeAffinity", "NoCompatibleDestination",
	"ConsumerControllerMissing", "UnsupportedWorkloadController", "ConsumerControllerChanged", "CustomSchedulerUnsupported",
	"SchedulingGateUnsupported", "InterPodAffinityUnsupported", "TopologySpreadUnsupported", "ResourceClaimUnsupported",
	"SchedulerVolumeUnsupported", "InvalidPodDisruptionBudget", "DisruptionBudgetDenied", "MultipleDisruptionBudgets", "Unknown",
}

func bounded(value string, allowed []string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return "Unknown"
}

func (e *Exporter) ObserveDiscovery(counts map[string]int, err error) {
	if err != nil {
		e.Cache.update("discovery", nil, false)
		return
	}
	normalized := make(map[string]int)
	for reason, count := range counts {
		normalized[bounded(reason, deferredReasons)] += count
	}
	values := make([]sample, 0, len(deferredReasons))
	for _, reason := range deferredReasons {
		values = append(values, sample{"mobility_deferred_volumes", float64(normalized[reason]), []string{reason}})
	}
	e.Cache.update("discovery", values, true)
}
