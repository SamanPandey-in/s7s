package apiserver

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"s7s/pkg/api"
	"s7s/pkg/store"
)

type Server struct {
	pods *store.PodStore
}

func New(pods *store.PodStore) *Server {
	return &Server{pods: pods}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	base := "/api/v1/namespaces/{ns}/pods"
	mux.HandleFunc("POST "+base, s.createPod)
	mux.HandleFunc("GET "+base, s.listPods)
	mux.HandleFunc("GET "+base+"/{name}", s.getPod)
	mux.HandleFunc("PUT "+base+"/{name}", s.updatePod)
	mux.HandleFunc("DELETE "+base+"/{name}", s.deletePod)

	return logRequests(mux)
}

func (s *Server) createPod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var pod api.Pod
	if err := dec.Decode(&pod); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := validatePod(&pod); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// server-owned fields, the client doesnt gets to set these
	pod.Kind = "Pod"
	pod.APIVersion = "v1"
	pod.Metadata.Namespace = ns
	pod.Metadata.UID = newUID()
	pod.Metadata.CreationTimestamp = time.Now().UTC()
	pod.Status = api.PodStatus{Phase: api.PodPending}

	if err := s.pods.Create(&pod); err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, pod)
}

func (s *Server) listPods(w http.ResponseWriter, r *http.Request) {
	list := api.PodList{
		TypeMeta: api.TypeMeta{Kind: "PodList", APIVersion: "v1"},
		Items:    s.pods.List(r.PathValue("ns")),
	}

	writeJSON(w, http.StatusOK, list)
}

func (s *Server) getPod(w http.ResponseWriter, r *http.Request) {
	pod, err := s.pods.Get(r.PathValue("ns"), r.PathValue("name"))

	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pod)
}

func (s *Server) updatePod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")

	// step 1: decode the body-same rules as create
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var pod api.Pod
	if err := dec.Decode(&pod); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := validatePod(&pod); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// step 2: identity comes from Path and not the body
	if pod.Metadata.Name != name {
		writeError(w, http.StatusBadRequest, "metadata.name must match the URL")
		return
	}

	// step 3: Load the old pod -> all its server owned fields must survive the replacePod
	old, err := s.pods.Get(ns, name)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	// step 4: keep Unchanged: UID + creationTimestamp (identity of this object)
	pod.Metadata.UID = old.Metadata.UID
	pod.Metadata.CreationTimestamp = old.Metadata.CreationTimestamp
	pod.Status = old.Status
	pod.Spec.NodeName = old.Spec.NodeName

	// step 5: reset status: we replaced the desired state, but the actual/observed state is still stale
	pod.Kind = "Pod"
	pod.APIVersion = "v1"
	pod.Metadata.Namespace = ns

	// step 6: writing it back
	if err := s.pods.Update(&pod); err != nil {
		writeStoreError(w, err)
		return
	}

	// step 7. 200, not 201 coz the object already existed, we replaced it
	writeJSON(w, http.StatusOK, pod)
}

func (s *Server) deletePod(w http.ResponseWriter, r *http.Request) {
	if err := s.pods.Delete(r.PathValue("ns"), r.PathValue("name")); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validatePod(p *api.Pod) error {
	if p.Metadata.Name == "" {
		return errors.New("metadata.name is required")
	}
	if len(p.Spec.Containers) == 0 {
		return errors.New("spec.containers must have atleast one container")
	}

	for i, c := range p.Spec.Containers {
		if c.Name == "" || c.Image == "" {
			return fmt.Errorf("spec.conatiners[%d]: name and image are required", i)
		}
	}

	return nil
}

func newUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
