package cache

import (
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"sync"
	"time"
)

const (
	expireInSeconds = 60 * 5
)

type MemoryCache struct {
	l                sync.RWMutex
	elements         map[string]cacheElement
	ElementSizeLimit int
	elementsLength   int
}

type cacheElement struct {
	value         []byte
	lastUsageTime time.Time
	usageCount    int
}

func MewMemoryCache() *MemoryCache {
	mc := &MemoryCache{
		elements:         make(map[string]cacheElement),
		ElementSizeLimit: 1024,
		elementsLength:   0,
	}
	ticker := time.NewTicker(time.Second * expireInSeconds)
	go func() {
		for {
			select {
			case <-ticker.C:
				mc.optimize()
			}
		}
	}()
	return mc
}

func (m *MemoryCache) Get(key string) string {
	m.l.RLock()
	item := ""
	if e, found := m.elements[key]; found {
		m.l.RUnlock()
		if err := msgpack.Unmarshal(e.value, &item); err != nil {
			panic(err)
		}
		e.usageCount += 1
		e.lastUsageTime = time.Now()
		m.l.Lock()
		m.elements[key] = e
		m.l.Unlock()
	} else {
		m.l.RUnlock()
	}
	return item
}

func (m *MemoryCache) Set(key, value string) {
	m.l.Lock()
	_, exists := m.elements[key]
	if !exists {
		m.elementsLength += 1
	}
	if m.elementsLength >= m.ElementSizeLimit {
		m.optimize()
	}
	valueBytes, err := msgpack.Marshal(value)
	if err != nil {
		panic(err)
	}
	m.elements[key] = cacheElement{
		value:         valueBytes,
		lastUsageTime: time.Now(),
		usageCount:    0,
	}
	m.l.Unlock()
}

func (m *MemoryCache) optimize() {
	log.Println("Starting to optimize memcache!")
	deletedCount := 0
	for key := range m.elements {
		secondsPassedSinceAdded := time.Now().Sub(m.elements[key].lastUsageTime).Seconds()
		if m.elements[key].usageCount == 0 && secondsPassedSinceAdded > 30 {
			delete(m.elements, key)
			deletedCount += 1
		}

		if secondsPassedSinceAdded > expireInSeconds {
			delete(m.elements, key)
			deletedCount += 1
		}
	}
	log.Printf("Optimizing finished, deleted %d keys!\n", deletedCount)
}
