package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

type Record struct {
	Offset       string
	CreationTime int64
}

type HashMapIndex struct {
	sync.RWMutex
	hm map[string]Record
}

func (m *HashMapIndex) AllKeys() []string {
	var keys []string
	for key := range m.hm {
		keys = append(keys, key)
	}
	return keys
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

func (m *HashMapIndex) Get(key string) (string, error) {
	m.RLock()
	defer m.RUnlock()
	data, exists := m.hm[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	return data.Offset, nil
}

func (m *HashMapIndex) GetCreationTime(key string) (int64, error) {
	m.RLock()
	defer m.RUnlock()
	data, exists := m.hm[key]
	if !exists {
		return 0, ErrKeyNotFound
	}
	return data.CreationTime, nil
}

func (m *HashMapIndex) Delete(key string) {
	delete(m.hm, key)
}

func (m *HashMapIndex) Set(key, offset string, creationTime int64) {
	m.Lock()
	defer m.Unlock()
	if _, exists := m.hm[key]; !exists && entryCount != nil {
		entryCount.Inc()
	}
	m.hm[key] = Record{
		Offset:       offset,
		CreationTime: creationTime,
	}
}

func NewHashMapIndex() Index {
	return &HashMapIndex{
		hm: map[string]Record{},
	}
}
