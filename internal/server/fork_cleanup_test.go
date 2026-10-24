package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

type forkSaveFailureStore struct{ store.Store }

func (s forkSaveFailureStore) SaveWorkspace(context.Context, *v1alpha1.Workspace) error {
	return errors.New("injected save failure")
}

func TestForkFailureCleanup(t *testing.T) {
	for _, tc := range []struct {
		name, phase, wantError, cleanupError                                               string
		cancel, deadline, pollError, conflict, replaced, deleteError, stuck, save, invalid bool
	}{
		{name: "operator failure", phase: "Failed", wantError: "injected operator failure"},
		{name: "poll failure", pollError: true, wantError: "500 Internal Server Error"},
		{name: "cancellation", cancel: true, wantError: "context canceled"},
		{name: "deadline", deadline: true, wantError: "context deadline exceeded"},
		{name: "incomplete Ready", phase: "Ready", invalid: true, wantError: "Ready without"},
		{name: "save failure", phase: "Ready", save: true, wantError: "injected save failure"},
		{name: "save and cleanup failure", phase: "Ready", save: true, deleteError: true, wantError: "injected save failure", cleanupError: "cleanup BrewFSWorkspaceFork: Kubernetes returned 403"},
		{name: "adopted Ready save failure", phase: "Ready", save: true, conflict: true, wantError: "injected save failure"},
		{name: "validation failure", phase: "Ready", save: true, invalid: true, wantError: "invalid workspace"},
		{name: "delete failure preserves original", phase: "Failed", deleteError: true, wantError: "injected operator failure", cleanupError: "cleanup BrewFSWorkspaceFork: Kubernetes returned 403"},
		{name: "finalizer timeout preserves original", phase: "Failed", stuck: true, wantError: "injected operator failure", cleanupError: "finalizer cleanup: context deadline exceeded"},
		{name: "adopted fork is not owned", phase: "Failed", conflict: true, wantError: "injected operator failure"},
		{name: "replacement is not owned", phase: "Failed", replaced: true, wantError: "conflict", cleanupError: "refusing deletion"},
		{name: "success keeps resources", phase: "Ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var resources, deleted []string
			gets, deletes := 0, 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.deadline {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer deadlineCancel()
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodPost:
					resources = []string{"fork", "snapshot", "workspace", "storageclass"}
					if tc.conflict {
						w.WriteHeader(http.StatusConflict)
						return
					}
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"metadata":{"uid":"owned-uid"}}`))
				case http.MethodGet:
					gets++
					if tc.deadline && gets == 1 {
						<-r.Context().Done()
						return
					}
					if len(resources) == 0 {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					if tc.pollError && gets == 1 {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					if tc.cancel && gets == 1 {
						cancel()
					}
					uid := "owned-uid"
					if tc.replaced {
						uid = "replacement-uid"
					}
					storageClass := "brewfs-child"
					if tc.invalid {
						storageClass = ""
						if tc.save {
							storageClass = "INVALID/class"
						}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"metadata": map[string]string{"uid": uid, "resourceVersion": "1", "namespace": "tenant"},
						"spec":     map[string]any{"clusterRef": map[string]string{"name": "cluster"}, "sourceWorkspaceRef": map[string]string{"name": "parent"}, "workspaceName": "child"},
						"status":   map[string]string{"phase": tc.phase, "message": "injected operator failure", "workspaceId": "child-id", "storageClassName": storageClass},
					})
				case http.MethodDelete:
					deletes++
					var options struct {
						Preconditions map[string]string `json:"preconditions"`
					}
					if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
						t.Error(err)
					}
					if !reflect.DeepEqual(options.Preconditions, map[string]string{"uid": "owned-uid", "resourceVersion": "1"}) {
						t.Errorf("unsafe delete: %+v", options)
					}
					if tc.deleteError {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					// Model the operator's finalizer contract. The CR must remain until
					// dependent resources are gone; AX must wait rather than just DELETE.
					w.WriteHeader(http.StatusAccepted)
				default:
					t.Errorf("unexpected method %s", r.Method)
				}
				if r.Method == http.MethodGet && deletes > 0 && !tc.stuck {
					deleted = append(deleted, "workspace", "snapshot", "storageclass", "fork")
					resources = nil
				}
			}))
			defer api.Close()
			f := &KubernetesWorkspaceForker{client: api.Client(), baseURL: api.URL, namespace: "tenant", clusterName: "cluster", cleanupTimeout: 500 * time.Millisecond}
			var err error
			if tc.save {
				st := memory.NewStore()
				if err := st.SaveWorkspace(context.Background(), &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "parent", Atespace: "tenant"}, Spec: func() *v1alpha1.WorkspaceSpec {
					if tc.invalid {
						return &v1alpha1.WorkspaceSpec{Files: []*v1alpha1.File{{}}}
					}
					return &v1alpha1.WorkspaceSpec{}
				}()}); err != nil {
					t.Fatal(err)
				}
				srv := NewServer(forkSaveFailureStore{st}, Options{Forker: f})
				_, err = srv.ForkWorkspace(ctx, &v1alpha1.ForkWorkspaceRequest{Atespace: "tenant", ParentName: "parent", Name: "child"})
			} else {
				_, err = f.Fork(ctx, "tenant", "parent", "child")
			}
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("original failure lost: %v; want %q", err, tc.wantError)
			}
			if tc.cleanupError != "" && (err == nil || !strings.Contains(err.Error(), tc.cleanupError)) {
				t.Fatalf("cleanup failure lost: %v", err)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Errorf("cancellation not preserved: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			shouldDelete := tc.wantError != "" && !tc.conflict && !tc.replaced
			if shouldDelete && deletes != 1 {
				t.Errorf("DELETE count = %d, want exactly one", deletes)
			}
			if !shouldDelete && deletes != 0 {
				t.Errorf("DELETE count = %d, want zero", deletes)
			}
			if shouldDelete && tc.cleanupError == "" {
				if len(resources) != 0 {
					t.Fatalf("resources leaked after fork failure: %v", resources)
				}
				if !reflect.DeepEqual(deleted, []string{"workspace", "snapshot", "storageclass", "fork"}) {
					t.Errorf("finalizer not completed: %v", deleted)
				}
			} else if len(resources) == 0 {
				t.Fatal("unexpected removal of resources")
			}
		})
	}
}
