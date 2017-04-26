// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/cannium/gohbase/region"
)

type clientCache struct {
	lock *sync.RWMutex
	// maps connection string(host:port) -> region client
	clients map[string]*region.Client
}

func newClientCache() *clientCache {
	return &clientCache{
		lock:    new(sync.RWMutex),
		clients: make(map[string]*region.Client),
	}
}

func (c *clientCache) get(host string, port uint16) *region.Client {
	connection := fmt.Sprintf("%s:%d", host, port)
	c.lock.RLock()
	client, hit := c.clients[connection]
	c.lock.RUnlock()
	if hit {
		return client
	} else {
		return nil
	}
}

func (c *clientCache) put(host string, port uint16, client *region.Client) {
	connection := fmt.Sprintf("%s:%d", host, port)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clients[connection] = client
}

func (c *clientCache) del(host string, port uint16) {
	connection := fmt.Sprintf("%s:%d", host, port)
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.clients, connection)
}

func (c *clientCache) closeAll() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, client := range c.clients {
		client.ClientDown()
	}
}

type regionCache struct {
	lock *sync.Mutex
	// Regions are organized in balanced binary trees
	// maps table -> region tree
	// type: string -> *region.Region
	regions map[string]*region.Region

	// `clients` caches region clients, excluding meta region client
	// and admin region client
	clients *clientCache
}

func newRegionCache() *regionCache {
	return &regionCache{
		lock:    new(sync.Mutex),
		regions: make(map[string]*region.Region),
		clients: newClientCache(),
	}
}

// Check if the binary tree for specific table exists, if not, create it
func (c *regionCache) getTree(table []byte) *region.Region {
	c.lock.Lock()
	defer c.lock.Unlock()

	tree, ok := c.regions[string(table)]
	if ok {
		return tree
	}

	tree = region.NewRegion(table, nil, []byte(""), []byte(""))
	c.regions[string(table)] = tree
	return tree
}

// return a working region for (table, key) pair
func (c *regionCache) get(ctx context.Context, table, key []byte,
	onUnavailable func(context.Context) (*region.Region, string, uint16, error)) (*region.Region, error) {

	tree := c.getTree(table)
	cachedRegion := tree.Find(key)
	// lock the region so other requests of same region would wait for the region
	// to become available
	cachedRegion.Lock()
	defer cachedRegion.Unlock()
	if cachedRegion.Available() {
		return cachedRegion, nil
	}

	// cachedRegion unavailable, find it from HBase
	fetchedRegion, host, port, err := onUnavailable(ctx)
	if err != nil {
		return nil, err
	}
	if rangeEqual(cachedRegion, fetchedRegion) {
		cachedRegion.SetName(fetchedRegion.Name())
		c.establishRegion(ctx, cachedRegion, host, port)
		return cachedRegion, nil
	}
	// TODO: support region merge
	cachedRegion.Split(fetchedRegion)
	c.establishRegion(ctx, fetchedRegion, host, port)
	return fetchedRegion, nil
}

// Mark region as unavailable, maintain both Region and Client
func markRegionUnavailable(r *region.Region) {
	client := r.MarkUnavailable()
	if client != nil {
		client.RemoveRegion(r)
	}
}

func (c *regionCache) establishRegion(ctx context.Context, r *region.Region,
	host string, port uint16) {

	// get client from cache
	client := c.clients.get(host, port)
	if client != nil {
		r.SetClient(client)
		client.AddRegion(r)
		return
	}

	// create new client if client not exists yet
	client = r.Connect(ctx, host, port)
	if client != nil {
		client.AddRegion(r)
		c.clients.put(host, port, client)
	}
}

func (c *regionCache) close() {
	c.clients.closeAll()
}

func rangeEqual(A, B *region.Region) bool {
	return bytes.Equal(A.StartKey(), B.StartKey()) &&
		bytes.Equal(A.StopKey(), B.StopKey())
}
