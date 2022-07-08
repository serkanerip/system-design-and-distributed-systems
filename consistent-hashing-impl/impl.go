package consistent_hashing_impl

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"log"
	"sort"
)

type node struct {
	ID       string
	Weight   int
	HashedID uint32
}

type nodes []node

type Ring struct {
	nodes nodes
}

func (n nodes) Len() int           { return len(n) }
func (n nodes) Less(i, j int) bool { return n[i].HashedID < n[j].HashedID }
func (n nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

func hashTheKey(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}

func (r *Ring) AddNode(ID string) {
	r.nodes = append(r.nodes, node{
		ID:       ID,
		Weight:   1,
		HashedID: hashTheKey(ID),
	})
	sort.Sort(r.nodes)
}

func (r *Ring) DeleteNode(ID string) {
	var tempArr []node
	for i := range r.nodes {
		if r.nodes[i].ID != ID {
			tempArr = append(tempArr, r.nodes[i])
		}
	}
	r.nodes = tempArr
}

func (r *Ring) Get(key string) string {
	keyHash := hashTheKey(key)
	fmt.Printf("hash for key %s is: %d\n", key, keyHash)
	for i := range r.nodes {
		if keyHash >= r.nodes[i].HashedID {
			return r.nodes[i].ID
		}
	}
	log.Println("couldn't map key with a node")
	return r.nodes[0].ID
}

func (r *Ring) GetNodes() []node {
	return r.nodes
}
