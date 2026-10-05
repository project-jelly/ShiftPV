package cleanupapi

import "github.com/project-jelly/ShiftPV/src/volume"

const ExecutorNode = "Node"

func (e Executor) ExecutionUID() string {
	if e.Kind == ExecutorNode {
		return e.PodUID
	}
	return e.JobUID
}
func (e Executor) Valid() bool {
	if !volume.ValidObjectName(e.NodeName) {
		return false
	}
	if e.Kind == ExecutorNode {
		return e.JobName == "" && e.JobUID == "" && volume.ValidObjectName(e.Namespace) && volume.ValidObjectName(e.PodName) && volume.ValidIdentityToken(e.PodUID)
	}
	return e.Kind == "" && e.Namespace == "" && e.PodName == "" && volume.ValidObjectName(e.JobName) && volume.ValidIdentityToken(e.JobUID)
}
