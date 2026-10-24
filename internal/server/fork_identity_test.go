package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestForkRejectsConflictFromDifferentSource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusConflict)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"spec": map[string]any{
					"clusterRef":         map[string]string{"name": "cluster-a"},
					"sourceWorkspaceRef": map[string]string{"name": "parent-b"},
					"workspaceName":      "child",
				},
				"status": map[string]string{
					"phase": "Ready", "workspaceId": "other-id", "storageClassName": "other-class",
				},
			})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	forker := &KubernetesWorkspaceForker{
		client: http.DefaultClient, baseURL: server.URL, namespace: "tenant-a", clusterName: "cluster-a",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := forker.Fork(ctx, "tenant-a", "parent-a", "child")
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("Fork() error = %v, want a conflict for an existing fork with a different source", err)
	}
}
