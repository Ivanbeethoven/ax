// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type workspaceLookupStore struct {
	store.Store
	lookup func(context.Context, string, string) (*v1alpha1.Workspace, error)
}

func (s *workspaceLookupStore) GetWorkspace(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error) {
	return s.lookup(ctx, atespace, name)
}

type workspaceForkerFunc func(context.Context, string, string, string) (server.ForkResult, error)

func (f workspaceForkerFunc) Fork(ctx context.Context, atespace, parentName, childName string) (server.ForkResult, error) {
	return f(ctx, atespace, parentName, childName)
}

func TestServer_ForkWorkspaceRechecksChildAfterLock(t *testing.T) {
	for _, childName := range []string{"a-child", "m-child", "z-child"} {
		t.Run(childName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			st := memory.NewStore()
			parents := []string{"b-parent", "y-parent"}
			for _, parent := range parents {
				if err := st.SaveWorkspace(ctx, &v1alpha1.Workspace{
					Metadata: &v1alpha1.ObjectMeta{Name: parent, Atespace: "default"},
					Spec: &v1alpha1.WorkspaceSpec{Files: []*v1alpha1.File{
						{Path: "parent.txt", Content: parent},
					}},
				}); err != nil {
					t.Fatal(err)
				}
			}

			// Hold both preflight reads until each has observed the child missing.
			// Subsequent reads must see the real store, including the winning fork.
			var missingReads atomic.Int32
			preflightDone := make(chan struct{})
			gatedStore := &workspaceLookupStore{Store: st, lookup: func(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error) {
				ws, err := st.GetWorkspace(ctx, atespace, name)
				if name == childName && errors.Is(err, store.ErrNotFound) {
					read := missingReads.Add(1)
					if read == 2 {
						close(preflightDone)
					}
					if read <= 2 {
						select {
						case <-preflightDone:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
				}
				return ws, err
			}}
			forkCalls := make(chan string, 2)
			forker := workspaceForkerFunc(func(_ context.Context, _, parent, _ string) (server.ForkResult, error) {
				forkCalls <- parent
				// An existing Kubernetes fork can be adopted after a 409, so a
				// second call would return the same storage class successfully.
				return server.ForkResult{StorageClassName: "brewfs-child"}, nil
			})
			locker := lock.NewMemoryLocker()
			type forkResult struct {
				parent string
				child  *v1alpha1.Workspace
				err    error
			}
			results := make(chan forkResult, 2)
			for _, parent := range parents {
				srv := server.NewServer(gatedStore, server.Options{Locker: locker, Forker: forker})
				go func() {
					child, err := srv.ForkWorkspace(ctx, &v1alpha1.ForkWorkspaceRequest{
						ParentName: parent, Name: childName,
					})
					results <- forkResult{parent: parent, child: child, err: err}
				}()
			}
			successes := make(map[string]*v1alpha1.Workspace)
			conflicts := 0
			for range parents {
				select {
				case result := <-results:
					switch status.Code(result.err) {
					case codes.OK:
						successes[result.parent] = result.child
					case codes.AlreadyExists:
						conflicts++
					default:
						t.Errorf("fork from %s: %v", result.parent, result.err)
					}
				case <-ctx.Done():
					t.Fatal("concurrent forks did not finish: possible lock deadlock")
				}
			}
			if len(successes) != 1 || conflicts != 1 {
				t.Errorf("got %d successful forks and %d AlreadyExists; want one of each", len(successes), conflicts)
			}
			if len(forkCalls) != 1 {
				t.Errorf("forker called %d times; want 1", len(forkCalls))
			}
			select {
			case winner := <-forkCalls:
				stored, err := st.GetWorkspace(ctx, "default", childName)
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(stored, successes[winner]) {
					t.Errorf("stored child was overwritten: got %v, want first fork %v", stored, successes[winner])
				}
			default:
				t.Error("forker was never called")
			}

			// All parent/child locks must also be released on the conflict path.
			for _, name := range append(parents, childName) {
				unlock, err := locker.Lock(ctx, "workspace", "default", name)
				if err != nil {
					t.Fatalf("workspace lock %q was not released: %v", name, err)
				}
				unlock()
			}
		})
	}

	t.Run("lookup_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		st := memory.NewStore()
		if err := st.SaveWorkspace(ctx, &v1alpha1.Workspace{
			Metadata: &v1alpha1.ObjectMeta{Name: "parent", Atespace: "default"},
			Spec:     &v1alpha1.WorkspaceSpec{},
		}); err != nil {
			t.Fatal(err)
		}
		childReads := 0
		failingStore := &workspaceLookupStore{Store: st, lookup: func(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error) {
			if name == "child" {
				childReads++
				if childReads == 2 {
					return nil, errors.New("store unavailable")
				}
			}
			return st.GetWorkspace(ctx, atespace, name)
		}}
		forker := workspaceForkerFunc(func(context.Context, string, string, string) (server.ForkResult, error) {
			t.Error("forker called after child lookup failed")
			return server.ForkResult{}, nil
		})
		locker := lock.NewMemoryLocker()
		srv := server.NewServer(failingStore, server.Options{Locker: locker, Forker: forker})
		if _, err := srv.ForkWorkspace(ctx, &v1alpha1.ForkWorkspaceRequest{ParentName: "parent", Name: "child"}); status.Code(err) != codes.Internal {
			t.Errorf("ForkWorkspace error = %v; want Internal", err)
		}
		if _, err := st.GetWorkspace(ctx, "default", "child"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("child lookup = %v; want ErrNotFound", err)
		}
		for _, name := range []string{"parent", "child"} {
			unlock, err := locker.Lock(ctx, "workspace", "default", name)
			if err != nil {
				t.Fatalf("workspace lock %q was not released after lookup failed: %v", name, err)
			}
			unlock()
		}
	})
}
