package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestKubernetesWorkspaceForkerTenantNamespace(t *testing.T) {
	for _, tt := range []struct {
		name, atespace, namespace string
		wantError                 bool
	}{
		{"cross tenant", "tenant-a", "tenant-b", true},
		{"default namespace is not a fallback", "tenant-a", "default", true},
		{"missing tenant", "", "tenant-a", true},
		{"missing namespace", "tenant-a", "", true},
		{"both missing", "", "", true},
		{"same tenant", "tenant-a", "tenant-a", false},
		{"explicit default", "default", "default", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var requests atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				prefix := "/apis/storage.brewfs.io/v1alpha1/namespaces/" + tt.namespace + "/brewfsworkspaceforks"
				switch r.Method {
				case http.MethodPost:
					if r.URL.Path != prefix {
						t.Errorf("POST path = %q, want %q", r.URL.Path, prefix)
					}
					w.WriteHeader(http.StatusCreated)
				case http.MethodGet:
					if r.URL.Path != prefix+"/child" {
						t.Errorf("GET path = %q, want child in %q", r.URL.Path, prefix)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]string{
						"phase": "Ready", "workspaceId": "child-id", "storageClassName": "brewfs-child",
					}})
				default:
					t.Errorf("unexpected method %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer api.Close()
			forker := &KubernetesWorkspaceForker{
				client: api.Client(), baseURL: api.URL, namespace: tt.namespace, clusterName: "cluster",
			}
			result, err := forker.Fork(context.Background(), tt.atespace, "parent", "child")
			if tt.wantError {
				if err == nil {
					t.Errorf("accepted fork from atespace %q into namespace %q: %+v", tt.atespace, tt.namespace, result)
				} else if !strings.Contains(err.Error(), "atespace") || !strings.Contains(err.Error(), "namespace") {
					t.Errorf("error must explain tenant/namespace mismatch: %v", err)
				}
				if n := requests.Load(); n != 0 {
					t.Errorf("rejected fork must not contact Kubernetes; got %d requests", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("same-tenant fork failed: %v", err)
			}
			if result.WorkspaceID != "child-id" || result.StorageClassName != "brewfs-child" || requests.Load() != 2 {
				t.Errorf("unexpected fork result %+v after %d requests", result, requests.Load())
			}
		})
	}
}
