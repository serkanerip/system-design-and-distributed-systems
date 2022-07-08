package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

type Index interface {
	Get(key string) string
	Set(key, offset string)
	Recover(key, value string)
	CollectPromMetrics()
}

type MemoryIndex struct {
	sync.RWMutex
	hm map[string]string
}

var (
	entryCount prometheus.Counter
)

func (m *MemoryIndex) CollectPromMetrics() {
	entryCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "index_hm_entry_count",
		Help: "The total number of entries in hashmap.",
	})
}

func (m *MemoryIndex) Recover(key, offset string) {
	m.Set(key, offset)
}

func (m *MemoryIndex) Get(key string) string {
	m.RLock()
	defer m.RUnlock()
	return m.hm[key]
}

func (m *MemoryIndex) Set(key, offset string) {
	m.Lock()
	defer m.Unlock()
	if _, exists := m.hm[key]; !exists {
		entryCount.Inc()
	}
	m.hm[key] = offset
}

func NewMemoryIndex() Index {
	return &MemoryIndex{
		hm: map[string]string{},
	}
}
