package store

import (
	"errors"
	"sort"
	"sync"

	"s7s/pkg/api"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

// PodStore is an in-memory thread-safe store, etcd replaces this later
type PodStore struct {
	mu   sync.RWMutex
	pods map[string]api.Pod // key: namespace/name

	// GO: map[KEY_TYPE]VALUE_TYPE
	// why use namespace/name??
	// In k8s, two namespaces n have Pods with the same name: eg. prod/nginx, test/nginx
	// if we use nginx, then one pod can overwrite the other one.
	// so our key here is like: key := pod.Namespace + "/" + pod.Name
}

func NewPodStore() *PodStore {
	return &PodStore{pods: make(map[string]api.Pod)}
}

func key(ns, name string) string { return ns + "/" + name }

// method to create a new pod
func (s *PodStore) Create(p *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(p.Metadata.Namespace, p.Metadata.Name)
	if _, exists := s.pods[k]; exists {
		return ErrAlreadyExists
	}

	s.pods[k] = *p
	return nil
}

func (s *PodStore) Get(ns, name string) (api.Pod, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := key(ns, name)
	if p, ok := s.pods[k]; ok {
		return p, nil
	}

	return api.Pod{}, ErrNotFound
	// GO: what is api.Pod{}?
	// creates a pod containing zero values for all fields
}

func (s *PodStore) List(ns string) []api.Pod {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

func (s *PodStore) Replace(p *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(p.Metadata.Namespace, p.Metadata.Name)

	if _, ok := s.pods[k]; !ok {
		return ErrNotFound
	}

	s.pods[k] = *p
	return nil
}

func (s *PodStore) Delete(ns, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(ns, name)
	if _, ok := s.pods[k]; !ok {
		return ErrNotFound
	}

	delete(s.pods, k)
	return nil
}
