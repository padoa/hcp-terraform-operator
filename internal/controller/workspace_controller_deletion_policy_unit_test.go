// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	tfc "github.com/hashicorp/go-tfe"
	"github.com/hashicorp/jsonapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	appv1alpha2 "github.com/hashicorp/hcp-terraform-operator/api/v1alpha2"
)

func TestIsAdoptableDestroyRun(t *testing.T) {
	cases := []struct {
		name string
		run  *tfc.Run
		want bool
	}{
		{"nil run", nil, false},
		{"apply run", &tfc.Run{IsDestroy: false, Status: tfc.RunPending}, false},
		{"destroy pending", &tfc.Run{IsDestroy: true, Status: tfc.RunPending}, true},
		{"destroy planning", &tfc.Run{IsDestroy: true, Status: tfc.RunPlanning}, true},
		{"destroy planned, awaiting confirmation", &tfc.Run{IsDestroy: true, Status: tfc.RunPlanned}, true},
		{"destroy applying", &tfc.Run{IsDestroy: true, Status: tfc.RunApplying}, true},
		{"destroy applied", &tfc.Run{IsDestroy: true, Status: tfc.RunApplied}, true},
		{"destroy planned and finished", &tfc.Run{IsDestroy: true, Status: tfc.RunPlannedAndFinished}, true},
		{"destroy errored", &tfc.Run{IsDestroy: true, Status: tfc.RunErrored}, false},
		{"destroy canceled", &tfc.Run{IsDestroy: true, Status: tfc.RunCanceled}, false},
		{"destroy discarded", &tfc.Run{IsDestroy: true, Status: tfc.RunDiscarded}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAdoptableDestroyRun(tc.run); got != tc.want {
				t.Fatalf("isAdoptableDestroyRun(%+v) = %v, want %v", tc.run, got, tc.want)
			}
		})
	}
}

// fakeTFC is the smallest HCP Terraform API surface deleteWorkspace touches:
// one workspace, its runs, and a counter of destroy runs created.
type fakeTFC struct {
	mu         sync.Mutex
	workspace  string
	currentRun string
	runs       map[string]*tfc.Run
	created    int
}

func newFakeTFC(workspace string, current *tfc.Run) *fakeTFC {
	return &fakeTFC{
		workspace:  workspace,
		currentRun: current.ID,
		runs:       map[string]*tfc.Run{current.ID: current},
	}
}

func (f *fakeTFC) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("TFP-API-Version", "2.5")
		w.Header().Set("TFP-AppName", "HCP Terraform")
		w.Header().Set("X-RateLimit-Limit", "30")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/api/v2/workspaces/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if strings.TrimPrefix(r.URL.Path, "/api/v2/workspaces/") != f.workspace {
			http.Error(w, `{"errors":[{"status":"404","title":"not found"}]}`, http.StatusNotFound)
			return
		}
		writeJSONAPI(w, http.StatusOK, &tfc.Workspace{ID: f.workspace, CurrentRun: &tfc.Run{ID: f.currentRun}})
	})
	mux.HandleFunc("/api/v2/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.created++
		run := &tfc.Run{
			ID:                   fmt.Sprintf("run-destroy-%d", f.created),
			IsDestroy:            true,
			Status:               tfc.RunPending,
			ConfigurationVersion: &tfc.ConfigurationVersion{ID: "cv-1"},
		}
		f.runs[run.ID] = run
		f.currentRun = run.ID
		writeJSONAPI(w, http.StatusCreated, run)
	})
	mux.HandleFunc("/api/v2/runs/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		run, ok := f.runs[strings.TrimPrefix(r.URL.Path, "/api/v2/runs/")]
		if !ok {
			http.Error(w, `{"errors":[{"status":"404","title":"not found"}]}`, http.StatusNotFound)
			return
		}
		writeJSONAPI(w, http.StatusOK, run)
	})
	return mux
}

func writeJSONAPI(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", jsonapi.MediaType)
	w.WriteHeader(status)
	if err := jsonapi.MarshalPayloadWithoutIncluded(w, v); err != nil {
		panic(err)
	}
}

// newDeletionFixture wires a Workspace CR marked for deletion to a fake
// Kubernetes API and a fake HCP Terraform API. failStatusUpdates makes the first
// N status writes fail, which is what turns one deletion into a run storm.
func newDeletionFixture(t *testing.T, tfcServer *fakeTFC, destroyRunID string, failStatusUpdates int) (*WorkspaceReconciler, *workspaceInstance, types.NamespacedName) {
	t.Helper()

	srv := httptest.NewServer(tfcServer.handler())
	t.Cleanup(srv.Close)
	tfClient, err := tfc.NewClient(&tfc.Config{Address: srv.URL, Token: "test"})
	if err != nil {
		t.Fatalf("tfc client: %v", err)
	}

	scheme := runtime.NewScheme()
	if err := appv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	now := metav1.Now()
	instance := &appv1alpha2.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "ws",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{workspaceFinalizer},
		},
		Spec: appv1alpha2.WorkspaceSpec{
			Organization:   "org",
			Name:           "ws",
			DeletionPolicy: appv1alpha2.DeletionPolicyDestroy,
		},
		Status: appv1alpha2.WorkspaceStatus{
			WorkspaceID:  tfcServer.workspace,
			DestroyRunID: destroyRunID,
		},
	}
	remaining := failStatusUpdates
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance).
		WithStatusSubresource(instance).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if remaining > 0 {
					remaining--
					return errors.New("the object has been modified; please apply your changes to the latest version and try again")
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	r := &WorkspaceReconciler{Client: k8sClient, Recorder: record.NewFakeRecorder(16), Scheme: scheme}
	nn := types.NamespacedName{Name: "ws", Namespace: "default"}
	w := &workspaceInstance{log: logr.Discard(), tfClient: HCPTerraformClient{Client: tfClient}}
	if err := k8sClient.Get(context.Background(), nn, &w.instance); err != nil {
		t.Fatalf("get instance: %v", err)
	}
	return r, w, nn
}

// A status write failing after the destroy run was created must not lead the
// next reconcile to create a second destroy run.
func TestDeleteWorkspaceDestroyReusesQueuedRunAfterStatusUpdateFailure(t *testing.T) {
	ctx := context.Background()
	server := newFakeTFC("ws-test", &tfc.Run{ID: "run-apply", IsDestroy: false, Status: tfc.RunApplied, ConfigurationVersion: &tfc.ConfigurationVersion{ID: "cv-0"}})
	r, w, nn := newDeletionFixture(t, server, "", 1)

	if err := r.deleteWorkspace(ctx, w); err == nil {
		t.Fatal("first reconcile: expected the failed status update to be returned")
	}
	if server.created != 1 {
		t.Fatalf("first reconcile: created %d destroy runs, want 1", server.created)
	}

	// Second reconcile starts from the persisted object, where destroyRunID is still empty.
	w2 := &workspaceInstance{log: logr.Discard(), tfClient: w.tfClient}
	if err := r.Get(ctx, nn, &w2.instance); err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if w2.instance.Status.DestroyRunID != "" {
		t.Fatalf("precondition: destroyRunID persisted as %q, want empty", w2.instance.Status.DestroyRunID)
	}
	if err := r.deleteWorkspace(ctx, w2); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if server.created != 1 {
		t.Fatalf("second reconcile: created %d destroy runs in total, want 1", server.created)
	}
	if w2.instance.Status.DestroyRunID != "run-destroy-1" {
		t.Fatalf("second reconcile: destroyRunID = %q, want run-destroy-1", w2.instance.Status.DestroyRunID)
	}
	if w2.instance.Status.Run == nil || w2.instance.Status.Run.ID != "run-destroy-1" {
		t.Fatalf("second reconcile: status.run = %+v, want run-destroy-1", w2.instance.Status.Run)
	}
}

// A newer non-destroy run must never be mistaken for the destroy run: tracking it
// would delete the workspace, and its resources' state, once that apply completes.
func TestDeleteWorkspaceDestroyDoesNotAdoptNonDestroyCurrentRun(t *testing.T) {
	ctx := context.Background()
	server := newFakeTFC("ws-test", &tfc.Run{ID: "run-apply-2", IsDestroy: false, Status: tfc.RunPending, ConfigurationVersion: &tfc.ConfigurationVersion{ID: "cv-2"}})
	server.runs["run-destroy-failed"] = &tfc.Run{ID: "run-destroy-failed", IsDestroy: true, Status: tfc.RunErrored, ConfigurationVersion: &tfc.ConfigurationVersion{ID: "cv-1"}}
	r, w, _ := newDeletionFixture(t, server, "run-destroy-failed", 0)

	if err := r.deleteWorkspace(ctx, w); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if w.instance.Status.DestroyRunID != "run-destroy-failed" {
		t.Fatalf("destroyRunID = %q, want the failed destroy run to stay tracked", w.instance.Status.DestroyRunID)
	}
	if server.created != 0 {
		t.Fatalf("created %d destroy runs, want 0 without a retry policy", server.created)
	}
}

// A newer destroy run started outside the operator is tracked instead of duplicated.
func TestDeleteWorkspaceDestroyAdoptsNewerDestroyRun(t *testing.T) {
	ctx := context.Background()
	server := newFakeTFC("ws-test", &tfc.Run{ID: "run-destroy-manual", IsDestroy: true, Status: tfc.RunPlanning, ConfigurationVersion: &tfc.ConfigurationVersion{ID: "cv-3"}})
	server.runs["run-destroy-failed"] = &tfc.Run{ID: "run-destroy-failed", IsDestroy: true, Status: tfc.RunErrored, ConfigurationVersion: &tfc.ConfigurationVersion{ID: "cv-1"}}
	r, w, _ := newDeletionFixture(t, server, "run-destroy-failed", 0)

	if err := r.deleteWorkspace(ctx, w); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if w.instance.Status.DestroyRunID != "run-destroy-manual" {
		t.Fatalf("destroyRunID = %q, want run-destroy-manual", w.instance.Status.DestroyRunID)
	}
	if server.created != 0 {
		t.Fatalf("created %d destroy runs, want 0", server.created)
	}
}
