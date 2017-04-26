// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/internal/pb"
	"github.com/golang/protobuf/proto"
)

// Call represents an HBase RPC call.
type RpcCall interface {
	Table() []byte
	Name() string
	Key() []byte
	SetRegionName(name []byte)
	Serialize() ([]byte, error)
	// Returns a newly created (default-state) protobuf in which to store the
	// response of this call.
	NewResponse() proto.Message

	ResultChan() chan RpcResult

	Context() context.Context

	SetFamilies(fam map[string][]string) error
	SetFilter(ft filter.Filter) error
}

// RpcResult is struct that will contain both the resulting message from an RPC
// call, and any errors that may have occurred related to making the RPC call.
type RpcResult struct {
	Msg   proto.Message
	Error error
}

type rpcBase struct {
	table []byte

	key []byte

	regionName []byte

	resultChannel chan RpcResult
	// Protects access to result channel.
	resultChannelLock sync.Mutex

	ctx context.Context
}

func (b *rpcBase) Context() context.Context {
	return b.ctx
}

func (b *rpcBase) SetRegionName(name []byte) {
	b.regionName = name
}

func (b *rpcBase) regionSpecifier() *pb.RegionSpecifier {
	regionType := pb.RegionSpecifier_REGION_NAME
	return &pb.RegionSpecifier{
		Type:  &regionType,
		Value: b.regionName,
	}
}

func applyOptions(call RpcCall, options ...func(RpcCall) error) error {
	for _, option := range options {
		err := option(call)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *rpcBase) Table() []byte {
	return b.table
}

func (b *rpcBase) Key() []byte {
	return b.key
}

func (b *rpcBase) ResultChan() chan RpcResult {
	b.resultChannelLock.Lock()
	if b.resultChannel == nil {
		// Buffered channels, so that if a writer thread sends a message (or
		// reports an error) after the deadline it doesn't block due to the
		// requesting thread having moved on.
		b.resultChannel = make(chan RpcResult, 1)
	}
	b.resultChannelLock.Unlock()
	return b.resultChannel
}

// Families is used as a parameter for request creation. Adds families constraint to a request.
func Families(fam map[string][]string) func(RpcCall) error {
	return func(g RpcCall) error {
		return g.SetFamilies(fam)
	}
}

// Filters is used as a parameter for request creation. Adds filters constraint to a request.
func Filters(fl filter.Filter) func(RpcCall) error {
	return func(g RpcCall) error {
		return g.SetFilter(fl)
	}
}

// TimeRange is used as a parameter for request creation. Adds TimeRange constraint to a request.
// It will get values in range [from, to[ ('to' is exclusive).
func TimeRange(from, to time.Time) func(RpcCall) error {
	return TimeRangeUint64(uint64(from.UnixNano()/1e6), uint64(to.UnixNano()/1e6))
}

// TimeRangeUint64 is used as a parameter for request creation.
// Adds TimeRange constraint to a request.
// from and to should be in milliseconds
// // It will get values in range [from, to[ ('to' is exclusive).
func TimeRangeUint64(from, to uint64) func(RpcCall) error {
	return func(g RpcCall) error {
		if from >= to {
			// or equal is becuase 'to' is exclusive
			return fmt.Errorf("'from' timestamp (%dms) is greater"+
				" or equal to 'to' timestamp (%dms)",
				from, to)
		}
		switch c := g.(type) {
		default:
			return errors.New("TimeRange option can only be used with Get or Scan queries.")
		case *Get:
			c.fromTimestamp = from
			c.toTimestamp = to
		case *Scan:
			c.fromTimestamp = from
			c.toTimestamp = to
		}
		return nil
	}
}

// MaxVersions is used as a parameter for request creation.
// Adds MaxVersions constraint to a request.
func MaxVersions(versions uint32) func(RpcCall) error {
	return func(g RpcCall) error {
		switch c := g.(type) {
		default:
			return errors.New("MaxVersions option can only be used with Get or Scan queries.")
		case *Get:
			c.maxVersions = versions
		case *Scan:
			c.maxVersions = versions
		}
		return nil
	}
}

// NumberOfRows is used as a parameter for request creation.
// Adds NumberOfRows constraint to a request.
func NumberOfRows(n uint32) func(RpcCall) error {
	return func(g RpcCall) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("NumberOfRows option can only be used with Scan queries.")
		}
		scan.numberOfRows = n
		return nil
	}
}

// Cell is the smallest level of granularity in returned results.
// Represents a single cell in HBase (a row will have one cell for every qualifier).
type Cell pb.Cell

// Result holds a slice of Cells as well as miscellaneous information about the response.
type Result struct {
	Cells  []*Cell
	Exists *bool
	Stale  *bool
	// Any other variables we want to include.
}

// ToLocalResult takes a protobuf Result type and converts it to our own
// Result type in constant time.
func ToLocalResult(pbr *pb.Result) *Result {
	if pbr == nil {
		return &Result{}
	}
	return &Result{
		// Should all be O(1) operations.
		Cells:  toLocalCells(pbr),
		Exists: pbr.Exists,
		Stale:  pbr.Stale,
	}
}

func toLocalCells(pbr *pb.Result) []*Cell {
	return *(*[]*Cell)(unsafe.Pointer(pbr))
}

// We can now define any helper functions on Result that we want.
