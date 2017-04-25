// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/cannium/gohbase/hrpc"
	"github.com/cannium/gohbase/internal/pb"
	"github.com/golang/protobuf/proto"
)

type Region struct {
	table []byte
	// The attributes above are supposed to be immutable.

	lock          sync.RWMutex
	name          []byte
	startKey      []byte
	stopKey       []byte
	splitKey      []byte // the key to determine left and right
	client        *Client
	availableLock sync.Mutex

	// Region is organized in a balanced binary tree
	left        *Region
	right       *Region
	parent      *Region
	leftHeight  int // height of left child
	rightHeight int // height of right child
}

// NewInfo creates a new region info
func NewRegion(table, name, startKey, stopKey []byte) *Region {
	return &Region{
		table:    table,
		name:     name,
		startKey: startKey,
		stopKey:  stopKey,
		// define height of leaf node's children to be -1 so height of
		// a node would always be max(leftHeight + rightHeight) + 1
		leftHeight:  -1,
		rightHeight: -1,
	}
}

// regionFromCell parses a KeyValue from the meta table and creates the
// corresponding Region object.
func regionFromCell(cell *hrpc.Cell) (*Region, error) {
	value := cell.Value
	if len(value) == 0 {
		return nil, fmt.Errorf("empty value in %q", cell)
	} else if value[0] != 'P' {
		return nil, fmt.Errorf("unsupported region info version %d in %q",
			value[0], cell)
	}
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"
	magic := binary.BigEndian.Uint32(value)
	if magic != pbufMagic {
		return nil, fmt.Errorf("invalid magic number in %q", cell)
	}
	regInfo := &pb.RegionInfo{}
	err := proto.UnmarshalMerge(value[4:len(value)-4], regInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to decode %q: %s", cell, err)
	}
	return NewRegion(regInfo.TableName.Qualifier, cell.Row,
		regInfo.StartKey, regInfo.EndKey), nil
}

// ParseRegionInfo parses the contents of a row from the meta table.
// It's guaranteed to return a region info and a host/port OR return an error.
func ParseRegionInfo(metaRow *hrpc.Result) (*Region, string, uint16, error) {
	var reg *Region
	var host string
	var port uint16

	for _, cell := range metaRow.Cells {
		switch string(cell.Qualifier) {
		case "regioninfo":
			var err error
			reg, err = regionFromCell(cell)
			if err != nil {
				return nil, "", 0, err
			}
		case "server":
			value := cell.Value
			if len(value) == 0 {
				continue // Empty during NSRE.
			}
			colon := bytes.IndexByte(value, ':')
			if colon < 1 { // Colon can't be at the beginning.
				return nil, "", 0,
					fmt.Errorf("broken meta: no colon found in info:server %q", cell)
			}
			host = string(value[:colon])
			portU64, err := strconv.ParseUint(string(value[colon+1:]), 10, 16)
			if err != nil {
				return nil, "", 0, err
			}
			port = uint16(portU64)
		default:
			// Other kinds of qualifiers: ignore them.
			// TODO: If this is the parent of a split region, there are two other
			// KVs that could be useful: `info:splitA' and `info:splitB'.
			// Need to investigate whether we can use those as a hint to update our
			// regions_cache with the daughter regions of the split.
		}
	}

	if reg == nil {
		// There was no region in the row in meta, this is really not
		// expected.
		err := fmt.Errorf("Meta seems to be broken, there was no region in %v",
			metaRow)
		return nil, "", 0, err
	} else if port == 0 { // Either both `host' and `port' are set, or both aren't.
		return nil, "", 0, fmt.Errorf("Meta doesn't have a server location in %v",
			metaRow)
	}

	return reg, host, port, nil
}

// Available returns true if this region is available
func (r *Region) Available() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.client != nil
}

func (r *Region) Lock() {
	r.availableLock.Lock()
}

func (r *Region) Unlock() {
	r.availableLock.Unlock()
}

func (r *Region) MarkUnavailable() *Client {
	r.lock.Lock()
	defer r.lock.Unlock()
	client := r.client
	r.client = nil
	return client
}

func (r *Region) String() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return fmt.Sprintf("*region.info{Table: %q, Name: %q, StopKey: %q}",
		r.table, r.name, r.stopKey)
}

// Name returns region name
func (r *Region) Name() []byte {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.name
}

func (r *Region) SetName(name []byte) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.name = name
}

// StopKey return region stop key
func (r *Region) StopKey() []byte {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.stopKey
}

// StartKey return region start key
func (r *Region) StartKey() []byte {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.startKey
}

// Table returns region table
func (r *Region) Table() []byte {
	return r.table
}

func (r *Region) Client() *Client {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.client
}

func (r *Region) SetClient(client *Client) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.client = client
}

func (r *Region) Connect(ctx context.Context, host string, port uint16) *Client {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.client != nil {
		// another routine already setups client for this region
		return r.client
	}
	client, err := NewClient(ctx, host, port, RegionClient, QUEUE_SIZE, FLUSH_INTERVAL)
	if err != nil {
		// TODO should log specific error
		return nil
	}
	r.client = client
	return client
}

func (r *Region) Find(key []byte) *Region {
	r.lock.RLock()

	if r.splitKey == nil {
		// this is a leaf node
		r.lock.RUnlock()
		return r
	}

	if bytes.Compare(key, r.splitKey) < 0 {
		left := r.left
		r.lock.RUnlock()
		return left.Find(key)
	} else {
		right := r.right
		r.lock.RUnlock()
		return right.Find(key)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func lock(regions ...*Region) {
	for _, region := range regions {
		region.lock.Lock()
	}
}

func unlock(regions ...*Region) {
	for _, region := range regions {
		region.lock.Unlock()
	}
}

func rLock(regions ...*Region) {
	for _, region := range regions {
		region.lock.RLock()
	}
}

func rUnlock(regions ...*Region) {
	for _, region := range regions {
		region.lock.RUnlock()
	}
}

// left-left case:
//         z                                z
//        / \                             /   \
//       y   T4   Right Rotate (z)       x      y
//      / \       - - - - - - - ->     /  \    /  \
//     x   T3                         T1  T2  T3  T4
//    / \
//  T1   T2

// left-right case:
//       z                               z
//      / \                             /  \
//     y   T4    Right Rotate(z)      x      y
//    / \        - - - - - - - ->    / \    / \
//  T1   x                          T1  T2 T3  T4
//      / \
//    T2   T3
func (z *Region) rotateRight() {
	// no need to lock z since it's locked in `rebalance`
	t4 := z.right
	y := z.left
	y.lock.Lock()
	var x, t1, t2, t3 *Region
	if y.leftHeight > y.rightHeight { // left-left case
		x = y.left
		t1 = x.left
		t2 = x.right
		t3 = y.right
	} else { // left-right case
		x = y.right
		t1 = y.left
		t2 = x.left
		t3 = x.right
	}
	x.lock.Lock()

	rLock(t1, t2)
	combine(x, t1, t2)
	rUnlock(t2, t1)
	rLock(t3, t4)
	combine(y, t3, t4)
	rUnlock(t4, t3)
	combine(z, x, y)
	unlock(x, y)
}

// right-right case:
//    z                                z
//   /  \                            /   \
//  T1   y     Left Rotate(z)       x      y
//      /  \   - - - - - - - ->    / \    / \
//     T2   x                     T1  T2 T3  T4
//         / \
//       T3  T4

// right-left case:
//     z                             z
//    / \                           /  \
//  T1   y     Left Rotate(z)     x      y
//      / \    - - - - - - ->    / \    / \
//     x   T4                  T1  T2  T3  T4
//    / \
//  T2   T3
func (z *Region) rotateLeft() {
	// no need to lock z since it's locked in `rebalance`
	t1 := z.left
	y := z.right
	y.lock.Lock()
	var x, t2, t3, t4 *Region
	if y.leftHeight > y.rightHeight { // right-left case
		x = y.left
		t2 = x.left
		t3 = x.right
		t4 = y.right
	} else { // right-right case
		x = y.right
		t2 = y.left
		t3 = x.left
		t4 = x.right
	}
	x.lock.Lock()

	rLock(t1, t2)
	combine(x, t1, t2)
	rUnlock(t2, t1)
	rLock(t3, t4)
	combine(y, t3, t4)
	rUnlock(t4, t3)
	combine(z, x, y)
	unlock(x, y)
}

// given three regions, a, b, c, combine them to
//       a
//      / \
//     b   c
// b and c should be neighbours(i.e. b.stopKey == c.startKey)
func combine(a, b, c *Region) {
	a.left = b
	a.right = c
	a.startKey = b.startKey
	a.stopKey = c.stopKey
	a.splitKey = c.startKey
	a.leftHeight = a.left.height()
	a.rightHeight = a.right.height()

	b.parent = a
	c.parent = a
}

// Rebalance the tree upwards from this region node
func (r *Region) rebalance() {
	r.lock.Lock()

	r.left.lock.RLock()
	r.leftHeight = r.left.height()
	r.left.lock.RUnlock()
	r.right.lock.RLock()
	r.rightHeight = r.right.height()
	r.right.lock.RUnlock()

	if r.leftHeight-r.rightHeight > 1 {
		r.rotateRight()
	} else if r.rightHeight-r.leftHeight > 1 {
		r.rotateLeft()
	}

	parent := r.parent
	r.lock.Unlock()
	if parent != nil {
		parent.rebalance()
	}
}

// Need RLock around
func (r *Region) height() int {
	return max(r.leftHeight, r.rightHeight) + 1
}

// Split current region node with fetched region info from HBase
func (r *Region) Split(fetched *Region) {
	r.lock.Lock()

	var splits []*Region
	if bytes.Compare(r.startKey, fetched.startKey) < 0 {
		l := NewRegion(r.table, nil, r.startKey, fetched.startKey)
		splits = append(splits, l)
	}
	splits = append(splits, fetched)
	if bytes.Compare(r.stopKey, fetched.stopKey) > 0 ||
		// []byte("") is the smallest for bytes.Compare, but it means +inf
		// for stop keys
		(len(r.stopKey) == 0 && len(fetched.stopKey) != 0) {
		r := NewRegion(r.table, nil, fetched.stopKey, r.stopKey)
		splits = append(splits, r)
	}

	r.client = nil
	if len(splits) == 2 {
		// region splits into 2 regions
		combine(r, splits[0], splits[1])
	} else {
		// region splits into 3 regions, 1 on left, 2 on right
		right := NewRegion(r.table, nil, splits[1].startKey, splits[2].stopKey)
		combine(right, splits[1], splits[2])
		combine(r, splits[0], right)
	}

	parent := r.parent
	r.lock.Unlock()
	if parent != nil {
		parent.rebalance()
	}
}
