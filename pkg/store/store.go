package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"s7s/pkg/api"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("conflict: object was modified, fetch the latest version and retry")
)

type watcher struct {
	ns string
	ch chan api.PodEvent
}

// OLD-m1: PodStore is an in-memory thread-safe store, etcd replaces this later
// UPDATE-m3: PodStore is our mini etcd: versiond, watchable, optionally persisted.
type PodStore struct {
	mu       sync.RWMutex
	pods     map[string]api.Pod // key: namespace/name
	rev      uint64             // global revision, bumped on every write
	watchers map[int]*watcher   // registered clients listening for changes
	nextID   int                // assigns watcher IDs
	path     string             // state file; "" means in-memory only

	// GO: map[KEY_TYPE]VALUE_TYPE
	// why use namespace/name??
	// In k8s, two namespaces n have Pods with the same name: eg. prod/nginx, test/nginx
	// if we use nginx, then one pod can overwrite the other one.
	// so our key here is like: key := pod.Namespace + "/" + pod.Name
}

func NewPodStore() *PodStore {
	return &PodStore{
		pods:     make(map[string]api.Pod),
		watchers: make(map[int]*watcher),
	}
}

type snapshot struct {
	Revision uint64    `json:"revision"`
	Pods     []api.Pod `json:"pods"`
}

func Open(path string) (*PodStore, error) {
	s := NewPodStore()
	if path == "" {
		return s, nil
	}

	s.path = path
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	s.rev = snap.Revision
	for _, p := range snap.Pods {
		s.pods[key(p.Metadata.Namespace, p.Metadata.Name)] = p
	}
	return s, nil
}

func key(ns, name string) string { return ns + "/" + name }

func (s *PodStore) nextVersion() string {
	s.rev++
	return strconv.FormatUint(s.rev, 10)
}

// method to create a new pod
func (s *PodStore) Create(p *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(p.Metadata.Namespace, p.Metadata.Name)
	if _, exists := s.pods[k]; exists {
		return ErrAlreadyExists
	}
	p.Metadata.ResourceVersion = s.nextVersion()
	s.pods[k] = *p
	s.notify(api.EventAdded, *p)
	return s.persist()
}

// update replaces a pod. If the caller sends a ResourceVersion it must watch the stored one,
// otherwise someone else changed the pod first (optimistc concurrency).
// An empty ResourceVersion means "overwote regardless"...
func (s *PodStore) Update(p *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(p.Metadata.Namespace, p.Metadata.Name)
	old, ok := s.pods[k]
	if !ok {
		return ErrNotFound
	}
	if v := p.Metadata.ResourceVersion; v != "" && v != old.Metadata.ResourceVersion {
		return ErrConflict
	}
	p.Metadata.ResourceVersion = s.nextVersion()
	s.pods[k] = *p
	s.notify(api.EventModified, *p)
	return s.persist()
}

func (s *PodStore) Delete(ns, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(ns, name)
	old, ok := s.pods[k]
	if !ok {
		return ErrNotFound
	}
	delete(s.pods, k)
	old.Metadata.ResourceVersion = s.nextVersion()
	s.notify(api.EventDeleted, old)
	return s.persist()
}

func (s *PodStore) Get(ns, name string) (api.Pod, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := key(ns, name)
	p, ok := s.pods[k]
	if !ok {
		return api.Pod{}, ErrNotFound
		// GO: what is api.Pod{}?
		// creates a pod containing zero values for all fields
	}

	return p, nil
}

func (s *PodStore) List(ns string) []api.Pod {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.listLocked(ns)
}

func (s *PodStore) listLocked(ns string) []api.Pod {
	items := make([]api.Pod, 0) // non-nill items array so JSON gives [] and not null
	for _, p := range s.pods {
		if p.Metadata.Namespace == ns {
			items = append(items, p)
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Metadata.Name < items[j].Metadata.Name
	})
	return items
}

// Watch first sends an ADDED event for every existing pod in ns, then live events.
// The channel is closed when ctx is done or the watcher falls behind..
func (s *PodStore) Watch(ctx context.Context, ns string) <-chan api.PodEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.listLocked(ns)
	w := &watcher{ns: ns, ch: make(chan api.PodEvent, len(existing)+64)}
	for _, p := range existing {
		w.ch <- api.PodEvent{Type: api.EventAdded, Object: p}
	}

	id := s.nextID
	s.nextID++
	s.watchers[id] = w

	go func() {
		<-ctx.Done()
		s.removeWatcher(id)
	}()
	return w.ch
}

func (s *PodStore) removeWatcher(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if w, ok := s.watchers[id]; ok {
		delete(s.watchers, id)
		close(w.ch)
	}
}

// notify must be called with s.mu held.
// Channels are only ever snet to or closed while hold the lock, so a send on a closed channel cannot happen.
func (s *PodStore) notify(t api.EventType, p api.Pod) {
	for id, w := range s.watchers {
		if w.ns != p.Metadata.Namespace {
			continue
		}

		select {
		case w.ch <- api.PodEvent{Type: t, Object: p}:
		default:
			// Slow consumer, Never block writers: drop it and let the client list and watch again
			delete(s.watchers, id)
			close(w.ch)
		}
	}
}

// persist must also be called with s.mu held. It writes a temp file and renames it,
// so a crash never leaves a half-written state file.
func (s *PodStore) persist() error {
	if s.path == "" {
		return nil
	}

	snap := snapshot{Revision: s.rev, Pods: make([]api.Pod, 0, len(s.pods))}
	for _, p := range s.pods {
		snap.Pods = append(snap.Pods, p)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// NOTE:
// 1. persist runs after the in-memory change, so if the disk write fails the memory and the file disagree...
// 2. Create and Update modify the pod you pass in, which is convenient but surprising..
