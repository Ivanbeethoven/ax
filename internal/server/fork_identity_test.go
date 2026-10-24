package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestForkIdentity(t *testing.T) {
	for _, tt := range []struct {
		name, namespace, cluster, source, child string
		changeAfterPoll, wantError              bool
	}{
		{name: "idempotent retry", namespace: "tenant-a", cluster: "cluster-a", source: "parent-a", child: "child"},
		{name: "different source", namespace: "tenant-a", cluster: "cluster-a", source: "parent-b", child: "child", wantError: true},
		{name: "different cluster", namespace: "tenant-a", cluster: "cluster-b", source: "parent-a", child: "child", wantError: true},
		{name: "different tenant", namespace: "tenant-b", cluster: "cluster-a", source: "parent-a", child: "child", wantError: true},
		{name: "missing namespace", cluster: "cluster-a", source: "parent-a", child: "child", wantError: true},
		{name: "different child", namespace: "tenant-a", cluster: "cluster-a", source: "parent-a", child: "other", wantError: true},
		{name: "missing identity", wantError: true},
		{name: "source changes while pending", namespace: "tenant-a", cluster: "cluster-a", source: "parent-a", child: "child", changeAfterPoll: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var gets atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					w.WriteHeader(http.StatusConflict)
				case http.MethodGet:
					n := gets.Add(1)
					source, phase := tt.source, "Ready"
					if tt.changeAfterPoll {
						if n == 1 {
							phase = "Pending"
						} else {
							source = "parent-b"
						}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"metadata": map[string]string{"namespace": tt.namespace},
						"spec": map[string]any{
							"clusterRef":         map[string]string{"name": tt.cluster},
							"sourceWorkspaceRef": map[string]string{"name": source},
							"workspaceName":      tt.child,
						},
						"status": map[string]string{"phase": phase, "workspaceId": "child-id", "storageClassName": "child-class"},
					})
				default:
					t.Errorf("unexpected method %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer api.Close()
			forker := &KubernetesWorkspaceForker{client: api.Client(), baseURL: api.URL, namespace: "tenant-a", clusterName: "cluster-a"}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := forker.Fork(ctx, "tenant-a", "parent-a", "child")
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), "conflict") {
					t.Fatalf("Fork() = %+v, %v; want identity conflict", result, err)
				}
				if result != (ForkResult{}) {
					t.Fatalf("conflict returned workspace: %+v", result)
				}
			} else if err != nil || result.WorkspaceID != "child-id" || result.StorageClassName != "child-class" {
				t.Fatalf("idempotent retry = %+v, %v", result, err)
			}
			wantGets := int32(1)
			if tt.changeAfterPoll {
				wantGets = 2
			}
			if gets.Load() != wantGets {
				t.Errorf("GET count = %d, want %d", gets.Load(), wantGets)
			}
		})
	}
}
