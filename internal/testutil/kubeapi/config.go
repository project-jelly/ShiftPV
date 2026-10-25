// Package kubeapi loads only explicitly selected disposable test credentials.
package kubeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"

	"k8s.io/client-go/rest"
)

// Config uses kubectl's flattened certificate identity in memory. It never
// falls back to an ambient context or includes credential bytes in errors.
func Config(ctx context.Context, path string) (*rest.Config, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("explicit absolute fixture KUBECONFIG is required")
	}
	data, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", path, "config", "view", "--raw", "--minify", "--flatten", "-o", "json").Output()
	if err != nil {
		return nil, fmt.Errorf("read fixture kubeconfig: %w", err)
	}
	var credentials struct {
		Clusters []struct {
			Cluster struct {
				Server string `json:"server"`
				CA     []byte `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
		Users []struct {
			User struct {
				Certificate []byte `json:"client-certificate-data"`
				Key         []byte `json:"client-key-data"`
			} `json:"user"`
		} `json:"users"`
	}
	if json.Unmarshal(data, &credentials) != nil || len(credentials.Clusters) != 1 || len(credentials.Users) != 1 || len(credentials.Users[0].User.Key) == 0 {
		return nil, fmt.Errorf("fixture requires one selected Kind certificate identity")
	}
	return &rest.Config{Host: credentials.Clusters[0].Cluster.Server, TLSClientConfig: rest.TLSClientConfig{
		CAData: credentials.Clusters[0].Cluster.CA, CertData: credentials.Users[0].User.Certificate, KeyData: credentials.Users[0].User.Key,
	}}, nil
}
