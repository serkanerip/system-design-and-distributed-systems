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

func (m *HashMapIndex) Get(key string) (string, error) {
	m.RLock()
	defer m.RUnlock()
	data, exists := m.hm[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	return data, nil
}

func (m *HashMapIndex) Set(key, offset string) {
	m.Lock()
	defer m.Unlock()
	if _, exists := m.hm[key]; !exists && entryCount != nil {
		entryCount.Inc()
	}
	m.hm[key] = offset
}

func NewHashMapIndex() Index {
	return &HashMapIndex{
		hm: map[string]string{},
	}
}
