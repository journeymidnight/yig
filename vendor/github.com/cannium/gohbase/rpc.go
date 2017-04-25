// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cannium/gohbase/hrpc"
	"github.com/cannium/gohbase/internal/zk"
	"github.com/cannium/gohbase/region"
	"github.com/golang/protobuf/proto"
)

// Constants
var (
	// Name of the meta region.
	metaTableName = []byte("hbase:meta")

	infoFamily = map[string][]string{
		"info": nil,
	}

	// ErrDeadline is returned when the deadline of a request has been exceeded
	ErrDeadline = errors.New("deadline exceeded")

	// TableNotFound is returned when attempting to access a table that
	// doesn't exist on this cluster.
	TableNotFound = errors.New("table not found")

	// Default timeouts

	// How long to wait for a region lookup (either meta lookup or finding
	// meta in ZooKeeper).  Should be greater than or equal to the ZooKeeper
	// session timeout.
	regionLookupTimeout = 30 * time.Second

	backoffStart = 16 * time.Millisecond
)

func (c *client) findDestinationRegion(rpc hrpc.RpcCall) (*region.Region, error) {
	if c.clientType == adminClient {
		return c.ensuredAdminRegion(rpc.Context())
	} else if bytes.Equal(rpc.Table(), metaTableName) {
		return c.ensureMetaRegion(rpc.Context())
	}

	onUnavailable := func(ctx context.Context) (*region.Region, string, uint16, error) {
		return c.metaLookup(ctx, rpc.Table(), rpc.Key())
	}

	return c.regions.get(rpc.Context(), rpc.Table(), rpc.Key(), onUnavailable)
}

func (c *client) sendRPC(rpc hrpc.RpcCall) (proto.Message, *region.Region, error) {
	remainingRetries := 10
	for {
		remainingRetries--
		r, err := c.findDestinationRegion(rpc)
		if err != nil {
			return nil, nil, err
		}

		rpc.SetRegionName(r.Name())
		client := r.Client()
		if client == nil {
			// it's possible that the region is marked unavailable before
			// we get its client
			continue
		}
		client.QueueRPC(rpc)

		// Wait for the response
		var result hrpc.RpcResult
		select {
		case result = <-rpc.ResultChan():
		case <-rpc.Context().Done():
			return nil, nil, ErrDeadline
		}

		if remainingRetries <= 0 {
			return result.Msg, r, result.Error
		}
		// Check for errors
		switch result.Error.(type) {
		case region.RetryableError:
			// Region is currently unavailable(split, moved, etc),
			// but our client(connection) is good, mark region as unavailable
			// and continue for loop
			markRegionUnavailable(r)
		case region.UnrecoverableError:
			// Client is considered dead(connection broken, etc),
			// mark ALL regions belong to this client as unavailable
			// and continue for loop
			r.Client().ClientDown()
		default:
			// RPC was successfully sent, or an unknown type of error
			// occurred. In either case, return the results.
			return result.Msg, r, result.Error
		}
	}
}

func (c *client) ensureRegionHelper(ctx context.Context, r *region.Region,
	zookeeperResource zk.ResourceName, clientType region.ClientType) error {

	if r.Available() {
		return nil
	}

	lookupContext, cancel := context.WithTimeout(ctx, regionLookupTimeout)
	host, port, err := c.zkLookup(lookupContext, zookeeperResource)
	cancel()
	if err != nil {
		return err
	}
	client, err := region.NewClient(ctx, host, port, clientType,
		region.QUEUE_SIZE, region.FLUSH_INTERVAL)
	if err != nil {
		return err
	}
	r.SetClient(client)
	client.AddRegion(r)
	return nil
}

func (c *client) ensuredAdminRegion(rootContext context.Context) (*region.Region, error) {
	err := c.ensureRegionHelper(rootContext, c.adminRegion, zk.Master, region.MasterClient)
	return c.adminRegion, err
}

func (c *client) ensureMetaRegion(rootContext context.Context) (*region.Region, error) {
	err := c.ensureRegionHelper(rootContext, c.metaRegion, zk.Meta, region.RegionClient)
	return c.metaRegion, err
}

// Creates the META key to search for in order to locate the given key.
func createRegionSearchKey(table, key []byte) []byte {
	metaKey := make([]byte, 0, len(table)+len(key)+3)
	metaKey = append(metaKey, table...)
	metaKey = append(metaKey, ',')
	metaKey = append(metaKey, key...)
	metaKey = append(metaKey, ',')
	// ':' is the first byte greater than '9'.  We always want to find the
	// entry with the greatest timestamp, so by looking right before ':'
	// we'll find it.
	metaKey = append(metaKey, ':')
	return metaKey
}

// metaLookup checks meta table for the region in which the given row key for the given table is.
func (c *client) metaLookup(ctx context.Context,
	table, key []byte) (*region.Region, string, uint16, error) {

	metaKey := createRegionSearchKey(table, key)
	rpc, err := hrpc.NewGetBefore(ctx, metaTableName, metaKey, hrpc.Families(infoFamily))
	if err != nil {
		return nil, "", 0, err
	}

	resp, err := c.Get(rpc)
	if err != nil {
		return nil, "", 0, err
	}
	if len(resp.Cells) == 0 {
		return nil, "", 0, TableNotFound
	}

	reg, host, port, err := region.ParseRegionInfo(resp)
	if err != nil {
		return nil, "", 0, err
	}
	if !bytes.Equal(table, reg.Table()) {
		// This would indicate a bug in HBase.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.StopKey()) != 0 &&
		bytes.Compare(key, reg.StopKey()) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	return reg, host, port, nil
}

func backoff(t time.Duration) time.Duration {
	// TODO: Revisit how we back off here.
	if t < 5000*time.Millisecond {
		return t * 2
	} else {
		return t + 5000*time.Millisecond
	}
}

func sleepAndIncreaseBackoff(ctx context.Context, t time.Duration) (time.Duration, error) {
	select {
	case <-time.After(t):
	case <-ctx.Done():
		return 0, ErrDeadline
	}
	return backoff(t), nil
}

// zkResult contains the result of a ZooKeeper lookup (when we're looking for
// the meta region or the HMaster).
type zkResult struct {
	host string
	port uint16
	err  error
}

// zkLookup asynchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookup(ctx context.Context, resource zk.ResourceName) (string, uint16, error) {
	// We make this a buffered channel so that if we stop waiting due to a
	// timeout, we won't block the zkLookupSync() that we start in a
	// separate goroutine.
	reschan := make(chan zkResult, 1)
	go func() {
		host, port, err := c.zkClient.LocateResource(resource)
		// This is guaranteed to never block as the channel is always buffered.
		reschan <- zkResult{host, port, err}
	}()
	select {
	case res := <-reschan:
		return res.host, res.port, res.err
	case <-ctx.Done():
		return "", 0, ErrDeadline
	}
}
