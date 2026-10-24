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
	rollback         *forkRollback
}

// KubernetesWorkspaceForker is the in-cluster implementation.  It uses the
// Kubernetes REST API directly to avoid coupling AX to a second Kubernetes
// client dependency; the operator remains the owner of all BrewFS resources.
type KubernetesWorkspaceForker struct {
	client         *http.Client
	baseURL        string
	namespace      string
	clusterName    string
	cleanupTimeout time.Duration
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

func (f *KubernetesWorkspaceForker) Fork(ctx context.Context, atespace string, parentName, childName string) (result ForkResult, err error) {
	// This forker serves a single BrewFS namespace. Only the identically named
	// AX atespace may use it; workspace names are not unique across tenants.
	if atespace == "" || f.namespace == "" || atespace != f.namespace {
		return ForkResult{}, fmt.Errorf("atespace %q is not authorized for BrewFS namespace %q: a nonempty matching atespace is required", atespace, f.namespace)
	}
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
	defer response.Body.Close()
	var created struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	var rollback *forkRollback
	if response.StatusCode == http.StatusCreated || response.StatusCode == http.StatusOK {
		if decodeErr := json.NewDecoder(response.Body).Decode(&created); decodeErr != nil || created.Metadata.UID == "" {
			return ForkResult{}, fmt.Errorf("create BrewFSWorkspaceFork: cleanup unavailable because ownership UID is missing or response is invalid (%v); resources may remain", decodeErr)
		}
		rollback = &forkRollback{forker: f, path: path, uid: created.Metadata.UID, parent: parentName, child: childName}
		defer func() {
			if err != nil {
				err = errors.Join(err, rollback.cleanup(ctx))
			}
		}()
	}
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK && response.StatusCode != http.StatusConflict {
		return ForkResult{}, fmt.Errorf("create BrewFSWorkspaceFork: Kubernetes returned %s", response.Status)
	}

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(10 * time.Minute)
	}
	for time.Now().Before(deadline) {
		result, phase, message, identity, err := f.getFork(ctx, path)
		if err != nil {
			return ForkResult{}, err
		}
		// Validate the same response whose status we consume, on every poll: a
		// fork may be changed or replaced while we wait for it to become ready.
		if (rollback != nil && identity.UID != rollback.uid) || identity.Namespace != f.namespace || identity.ClusterName != f.clusterName || identity.SourceWorkspace != parentName || identity.WorkspaceName != childName {
			return ForkResult{}, fmt.Errorf("BrewFSWorkspaceFork %q conflict: existing fork identity does not match requested namespace %q, cluster %q, source %q, workspace %q", childName, f.namespace, f.clusterName, parentName, childName)
		}
		switch phase {
		case "Ready":
			if result.WorkspaceID == "" || result.StorageClassName == "" {
				return ForkResult{}, errors.New("BrewFSWorkspaceFork Ready without workspaceId/storageClassName")
			}
			result.rollback = rollback
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

type forkIdentity struct {
	UID             string
	ResourceVersion string
	Namespace       string
	ClusterName     string
	SourceWorkspace string
	WorkspaceName   string
}

func (f *KubernetesWorkspaceForker) getFork(ctx context.Context, path string) (ForkResult, string, string, forkIdentity, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.baseURL+path, nil)
	if err != nil {
		return ForkResult{}, "", "", forkIdentity{}, err
	}
	response, err := f.client.Do(request)
	if err != nil {
		return ForkResult{}, "", "", forkIdentity{}, fmt.Errorf("get BrewFSWorkspaceFork: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return ForkResult{}, "", "", forkIdentity{}, errForkNotFound
	}
	if response.StatusCode != http.StatusOK {
		return ForkResult{}, "", "", forkIdentity{}, fmt.Errorf("get BrewFSWorkspaceFork: Kubernetes returned %s", response.Status)
	}
	var object struct {
		Metadata struct {
			Namespace       string `json:"namespace"`
			UID             string `json:"uid"`
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Spec struct {
			ClusterRef struct {
				Name string `json:"name"`
			} `json:"clusterRef"`
			SourceWorkspaceRef struct {
				Name string `json:"name"`
			} `json:"sourceWorkspaceRef"`
			WorkspaceName string `json:"workspaceName"`
		} `json:"spec"`
		Status struct {
			Phase            string `json:"phase"`
			Message          string `json:"message"`
			WorkspaceID      string `json:"workspaceId"`
			StorageClassName string `json:"storageClassName"`
		} `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&object); err != nil {
		return ForkResult{}, "", "", forkIdentity{}, fmt.Errorf("decode BrewFSWorkspaceFork: %w", err)
	}
	identity := forkIdentity{UID: object.Metadata.UID, ResourceVersion: object.Metadata.ResourceVersion, Namespace: object.Metadata.Namespace, ClusterName: object.Spec.ClusterRef.Name, SourceWorkspace: object.Spec.SourceWorkspaceRef.Name, WorkspaceName: object.Spec.WorkspaceName}
	return ForkResult{WorkspaceID: object.Status.WorkspaceID, StorageClassName: object.Status.StorageClassName}, object.Status.Phase, object.Status.Message, identity, nil
}

// forkRollback is issued only for a successful create, never a 409 adoption.
// The operator owns dependent resources: deleting the fork starts its finalizer,
// which removes the child before its source snapshot, then the StorageClass.
// Waiting for CR disappearance ensures that finalizer cleanup actually completed.
type forkRollback struct {
	forker                   *KubernetesWorkspaceForker
	path, uid, parent, child string
}

func (r *forkRollback) cleanup(requestCtx context.Context) error {
	if r == nil {
		return nil
	}
	f := r.forker
	budget := f.cleanupTimeout
	if budget <= 0 {
		budget = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), budget)
	defer cancel()
	// Read identity and use resourceVersion as well as UID to close the race
	// between checking spec identity and issuing DELETE.
	_, _, _, identity, err := f.getFork(ctx, r.path)
	if errors.Is(err, errForkNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cleanup BrewFSWorkspaceFork: %w", err)
	}
	if identity.UID != r.uid || identity.Namespace != f.namespace || identity.ClusterName != f.clusterName || identity.SourceWorkspace != r.parent || identity.WorkspaceName != r.child || identity.ResourceVersion == "" {
		return errors.New("cleanup BrewFSWorkspaceFork: identity changed or incomplete; refusing deletion")
	}
	body, err := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{"uid": r.uid, "resourceVersion": identity.ResourceVersion}})
	if err != nil {
		return fmt.Errorf("cleanup BrewFSWorkspaceFork: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, f.baseURL+r.path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cleanup BrewFSWorkspaceFork: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("cleanup BrewFSWorkspaceFork: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("cleanup BrewFSWorkspaceFork: Kubernetes returned %s", resp.Status)
	}
	for {
		_, _, _, identity, err := f.getFork(ctx, r.path)
		if errors.Is(err, errForkNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("waiting for BrewFSWorkspaceFork finalizer cleanup: %w", err)
		}
		// Original CR is gone; never delete its replacement.
		if identity.UID != r.uid {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for BrewFSWorkspaceFork finalizer cleanup: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

var errForkNotFound = errors.New("BrewFSWorkspaceFork not found")
