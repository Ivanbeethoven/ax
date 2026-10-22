package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// WorkspaceForker provisions a BrewFSWorkspaceFork and waits until the
// operator has published the child workspace's StorageClass.  AX keeps this
// interface small so the API can be unit-tested without a Kubernetes cluster.
type WorkspaceForker interface {
	Fork(ctx context.Context, atespace, parentName, childName string) (ForkResult, error)
}

type ForkResult struct {
	WorkspaceID      string
	StorageClassName string
}

// KubernetesWorkspaceForker is the in-cluster implementation.  It uses the
// Kubernetes REST API directly to avoid coupling AX to a second Kubernetes
// client dependency; the operator remains the owner of all BrewFS resources.
type KubernetesWorkspaceForker struct {
	client      *http.Client
	baseURL     string
	namespace   string
	clusterName string
}

func NewInClusterWorkspaceForker() (*KubernetesWorkspaceForker, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host == "" {
		return nil, errors.New("KUBERNETES_SERVICE_HOST is not set")
	}
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read Kubernetes service-account token: %w", err)
	}
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read Kubernetes service-account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("parse Kubernetes service-account CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}}
	namespace := os.Getenv("BREWFS_NAMESPACE")
	if namespace == "" {
		namespace = os.Getenv("POD_NAMESPACE")
	}
	if namespace == "" {
		namespace = "default"
	}
	clusterName := os.Getenv("BREWFS_CLUSTER_NAME")
	if clusterName == "" {
		return nil, errors.New("BREWFS_CLUSTER_NAME is not set")
	}
	return &KubernetesWorkspaceForker{
		client:      &http.Client{Transport: bearerTransport{base: transport, token: string(token)}},
		baseURL:     "https://" + host + ":" + port,
		namespace:   namespace,
		clusterName: clusterName,
	}, nil
}

// bearerTransport adds the projected service-account token without exposing it
// through request arguments or logs.
type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (t bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}

func (f *KubernetesWorkspaceForker) Fork(ctx context.Context, _ string, parentName, childName string) (ForkResult, error) {
	if parentName == "" || childName == "" {
		return ForkResult{}, errors.New("parent and child workspace names are required")
	}
	if parentName == childName {
		return ForkResult{}, errors.New("child workspace must differ from parent")
	}
	path := "/apis/storage.brewfs.io/v1alpha1/namespaces/" + url.PathEscape(f.namespace) + "/brewfsworkspaceforks/" + url.PathEscape(childName)
	body := map[string]any{
		"apiVersion": "storage.brewfs.io/v1alpha1",
		"kind":       "BrewFSWorkspaceFork",
		"metadata":   map[string]string{"name": childName},
		"spec": map[string]any{
			"clusterRef":         map[string]string{"name": f.clusterName},
			"sourceWorkspaceRef": map[string]string{"name": parentName},
			"workspaceName":      childName,
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return ForkResult{}, fmt.Errorf("encode workspace fork: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.baseURL+"/apis/storage.brewfs.io/v1alpha1/namespaces/"+url.PathEscape(f.namespace)+"/brewfsworkspaceforks", bytes.NewReader(encoded))
	if err != nil {
		return ForkResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := f.client.Do(request)
	if err != nil {
		return ForkResult{}, fmt.Errorf("create BrewFSWorkspaceFork: %w", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK && response.StatusCode != http.StatusConflict {
		return ForkResult{}, fmt.Errorf("create BrewFSWorkspaceFork: Kubernetes returned %s", response.Status)
	}

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(10 * time.Minute)
	}
	for time.Now().Before(deadline) {
		result, phase, message, err := f.getFork(ctx, path)
		if err != nil {
			return ForkResult{}, err
		}
		switch phase {
		case "Ready":
			if result.WorkspaceID == "" || result.StorageClassName == "" {
				return ForkResult{}, errors.New("BrewFSWorkspaceFork Ready without workspaceId/storageClassName")
			}
			return result, nil
		case "Failed":
			return ForkResult{}, fmt.Errorf("BrewFSWorkspaceFork failed: %s", message)
		}
		select {
		case <-ctx.Done():
			return ForkResult{}, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return ForkResult{}, fmt.Errorf("timed out waiting for BrewFSWorkspaceFork %q", childName)
}

func (f *KubernetesWorkspaceForker) getFork(ctx context.Context, path string) (ForkResult, string, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.baseURL+path, nil)
	if err != nil {
		return ForkResult{}, "", "", err
	}
	response, err := f.client.Do(request)
	if err != nil {
		return ForkResult{}, "", "", fmt.Errorf("get BrewFSWorkspaceFork: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ForkResult{}, "", "", fmt.Errorf("get BrewFSWorkspaceFork: Kubernetes returned %s", response.Status)
	}
	var object struct {
		Status struct {
			Phase            string `json:"phase"`
			Message          string `json:"message"`
			WorkspaceID      string `json:"workspaceId"`
			StorageClassName string `json:"storageClassName"`
		} `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&object); err != nil {
		return ForkResult{}, "", "", fmt.Errorf("decode BrewFSWorkspaceFork: %w", err)
	}
	return ForkResult{WorkspaceID: object.Status.WorkspaceID, StorageClassName: object.Status.StorageClassName}, object.Status.Phase, object.Status.Message, nil
}
