// provisioning measures direct CSI calls in an isolated test installation.
// It creates no PVC/Pod and excludes scheduler, provisioner and CDI latency.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type sample struct {
	Case     string  `json:"case"`
	Name     string  `json:"name"`
	VolumeID string  `json:"volumeID"`
	Seconds  float64 `json:"seconds"`
	Code     string  `json:"code"`
}

type profile struct {
	client   csi.ControllerClient
	registry *volumeapi.Registry
	mode     string
	node     string
	group    string
	other    string
	prefix   string
	count    int
	mu       sync.Mutex
	created  map[string]*csi.Volume
	groups   map[string]string
	encoder  *json.Encoder
}

func main() {
	p := &profile{created: make(map[string]*csi.Volume), groups: make(map[string]string), encoder: json.NewEncoder(os.Stdout)}
	endpoint := flag.String("endpoint", "unix:///run/csi/csi.sock", "isolated installation CSI endpoint")
	flag.StringVar(&p.mode, "case", "new", "new, retry, same-pool or different-pool")
	flag.StringVar(&p.node, "node", "", "selected test node")
	flag.StringVar(&p.group, "group", "default", "first Pool group")
	flag.StringVar(&p.other, "other-group", "", "second independent Pool group on the same node")
	flag.IntVar(&p.count, "count", 10, "iterations (two calls per concurrent iteration)")
	flag.Parse()
	if err := p.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	api, err := dynamic.NewForConfig(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	p.registry = &volumeapi.Registry{Client: api}
	connection, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer connection.Close()
	p.client = csi.NewControllerClient(connection)
	p.prefix = fmt.Sprintf("profile-%s-%d", p.mode, time.Now().UnixNano())
	err = p.run(context.Background())
	cleanupErr := p.cleanup()
	if err != nil || cleanupErr != nil {
		fmt.Fprintf(os.Stderr, "measurement: %v; cleanup: %v\n", err, cleanupErr)
		os.Exit(1)
	}
}

func (p *profile) validate() error {
	if p.node == "" || p.count <= 0 {
		return fmt.Errorf("node and positive count are required")
	}
	switch p.mode {
	case "new", "retry", "same-pool":
	case "different-pool":
		if p.other == "" || p.other == p.group {
			return fmt.Errorf("different-pool needs a distinct other-group")
		}
	default:
		return fmt.Errorf("unknown case %q", p.mode)
	}
	return nil
}

func (p *profile) create(ctx context.Context, name, group string, report bool) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	request := &csi.CreateVolumeRequest{Name: name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 << 20},
		Parameters:    map[string]string{controller.PoolGroupKey: group},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
		AccessibilityRequirements: &csi.TopologyRequirement{Preferred: []*csi.Topology{{Segments: map[string]string{controller.TopologyKey: p.node}}}},
	}
	started := time.Now()
	response, err := p.client.CreateVolume(ctx, request)
	elapsed := time.Since(started)
	p.mu.Lock()
	defer p.mu.Unlock()
	result := sample{Case: p.mode, Name: name, Seconds: elapsed.Seconds(), Code: status.Code(err).String()}
	if err == nil {
		v := response.GetVolume()
		if v == nil || v.VolumeId == "" || v.CapacityBytes != 8<<20 || v.VolumeContext[controller.NodeContextKey] != p.node {
			return fmt.Errorf("unexpected volume response: %v", response)
		}
		result.VolumeID = v.VolumeId
		if prior := p.created[v.VolumeId]; prior != nil && !reflect.DeepEqual(prior, v) {
			return fmt.Errorf("retry changed volume response")
		}
		p.created[v.VolumeId] = v
		p.groups[v.VolumeId] = group
	}
	if report {
		if encodeErr := p.encoder.Encode(result); encodeErr != nil {
			return encodeErr
		}
	}
	return err
}

func (p *profile) run(ctx context.Context) error {
	var before volumeapi.State
	if p.mode == "retry" {
		if err := p.create(ctx, p.prefix, p.group, false); err != nil {
			return err
		}
		for id := range p.created {
			var err error
			before, err = p.registry.Get(ctx, id)
			if err != nil || before.Phase != volumeapi.PhaseReady || !volumeapi.ValidCreationReceipt(before) {
				return fmt.Errorf("seed has no Ready creation receipt: %v", err)
			}
		}
	}
	if err := p.encoder.Encode(map[string]string{"startedAt": time.Now().UTC().Format(time.RFC3339Nano), "case": p.mode}); err != nil {
		return err
	}
	for i := 0; i < p.count; i++ {
		name := fmt.Sprintf("%s-%d", p.prefix, i)
		if p.mode == "retry" {
			name = p.prefix
		}
		if p.mode == "new" || p.mode == "retry" {
			if err := p.create(ctx, name, p.group, true); err != nil {
				return err
			}
			continue
		}
		group := p.group
		if p.mode == "different-pool" {
			group = p.other
		}
		results := make(chan error, 2)
		go func() { results <- p.create(ctx, name+"-a", p.group, true) }()
		go func() { results <- p.create(ctx, name+"-b", group, true) }()
		first, second := <-results, <-results
		if first != nil || second != nil {
			return fmt.Errorf("concurrent calls: %v; %v", first, second)
		}
	}
	if err := p.encoder.Encode(map[string]string{"finishedAt": time.Now().UTC().Format(time.RFC3339Nano), "case": p.mode}); err != nil {
		return err
	}
	for id := range p.created {
		after, err := p.registry.Get(ctx, id)
		if err != nil || after.Phase != volumeapi.PhaseReady || !volumeapi.ValidCreationReceipt(after) {
			return fmt.Errorf("volume has no Ready creation receipt: %s: %v", id, err)
		}
		if p.mode == "retry" && !reflect.DeepEqual(before, after) {
			return fmt.Errorf("retry changed durable identity, capacity or receipt")
		}
		copy := after.CurrentCopy
		if copy == nil || copy.NodeName != p.node {
			return fmt.Errorf("volume has no copy on the selected node: %s", id)
		}
		pool, err := p.registry.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
		if err != nil {
			return fmt.Errorf("read selected Pool for %s: %w", id, err)
		}
		group := pool.PoolGroup
		if group == "" {
			group = volumeapi.DefaultPoolGroup
		}
		if group != p.groups[id] {
			return fmt.Errorf("volume %s belongs to group %q, expected %q", id, group, p.groups[id])
		}
	}
	return nil
}

func (p *profile) cleanup() error {
	for id := range p.created {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		for {
			_, err := p.client.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id})
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				cancel()
				return fmt.Errorf("delete %s: %w", id, err)
			case <-time.After(time.Second):
			}
		}
		cancel()
	}
	return nil
}
