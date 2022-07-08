package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

type HashMapIndex struct {
	sync.RWMutex
	hm map[string]string
}

var (
	entryCount prometheus.Counter
)

func (m *HashMapIndex) CollectPromMetrics() {
	entryCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "index_hm_entry_count",
		Help: "The total number of entries in hashmap.",
	})
}

func (m *HashMapIndex) Recover(key, offset string) {
	m.Set(key, offset)
}

func (m *HashMapIndex) Get(key string) string {
	m.RLock()
	defer m.RUnlock()
	return m.hm[key]
}

func (m *HashMapIndex) Set(key, offset string) {
	m.Lock()
	defer m.Unlock()
	if _, exists := m.hm[key]; !exists {
		entryCount.Inc()
	}
	m.hm[key] = offset
}

func NewMemoryIndex() Index {
	return &HashMapIndex{
		hm: map[string]string{},
	}
}
