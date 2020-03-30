package redis

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"sync"
)

// nodeIdx type implementing Sort Interface
type nodeIdx []uint64

// Len returns the size of nodeIdx
func (idx nodeIdx) Len() int {
	return len(idx)
}

// Swap swaps the ith with jth
func (idx nodeIdx) Swap(i, j int) {
	idx[i], idx[j] = idx[j], idx[i]
}

// Less returns true if ith <= jth else false
func (idx nodeIdx) Less(i, j int) bool {
	return idx[i] <= idx[j]
}

// HashRing to hold the nodes and indexes
type HashRing struct {
	nodes        map[uint64]interface{} // map to idx -> node
	idx          nodeIdx                // sorted indexes
	replicaCount int                    // replicas to be inserted
	hash         hash.Hash64
	mu           sync.RWMutex // to protect above fields
}

// New returns a Hash ring with provided virtual node count and hash

// If hash is nil, fvn64a is used instead
func NewHashRing(replicaCount int, hash hash.Hash64) *HashRing {
	if hash == nil {
		hash = fnv.New64a()
	}

	return &HashRing{
		nodes:        make(map[uint64]interface{}),
		replicaCount: replicaCount,
		hash:         hash,
	}
}


// getHash returns uint64 hash
func getHash(hash hash.Hash64, key []byte) (uint64, error) {
	hash.Reset()
	_, err := hash.Write(key)
	if err != nil {
		return 0, err
	}

	return hash.Sum64(), nil
}

// Add adds a node to Hash ring
func (hr *HashRing) Add(node interface{}) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.replicaCount; i++ {
		key := fmt.Sprintf("%s:%d", node, i)
		hkey, err := getHash(hr.hash, []byte(key))
		if err != nil {
			return fmt.Errorf("failed to add node: %v", err)
		}

		hr.idx = append(hr.idx, hkey)
		hr.nodes[hkey] = node
	}

	sort.Sort(hr.idx)
	return nil
}

// getKeys returns the keys of map m
func getKeys(m map[uint64]interface{}) (idx nodeIdx) {
	for k := range m {
		idx = append(idx, k)
	}

	return idx
}

// Delete deletes the nodes from hash ring
func (hr *HashRing) Delete(node interface{}) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.replicaCount; i++ {
		key := fmt.Sprintf("%s:%d", node, i)
		hkey, err := getHash(hr.hash, []byte(key))
		if err != nil {
			return fmt.Errorf("failed to delete node: %v", err)
		}

		delete(hr.nodes, hkey)
	}

	hr.idx = getKeys(hr.nodes)
	sort.Sort(hr.idx)
	return nil
}

// Locate returns the node for a given key
func (hr *HashRing) Locate(key string) (node interface{}, err error) {
	if len(hr.idx) < 1 {
		return node, fmt.Errorf("no available nodes")
	}

	hkey, err := getHash(hr.hash, []byte(key))
	if err != nil {
		return node, fmt.Errorf("failed to fetch node: %v", err)
	}

	pos := sort.Search(len(hr.idx), func(i int) bool { return hr.idx[i] >= hkey })
	if pos == len(hr.idx) {
		return hr.nodes[hr.idx[0]], nil
	}
	return hr.nodes[hr.idx[pos]], nil
}
