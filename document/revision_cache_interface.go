/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package document

import (
	"context"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// DefaultRevisionCacheSize is the number of recently-accessed doc revisions to cache in RAM
	DefaultRevisionCacheSize = 5000

	// DefaultRevisionCacheShardCount is the default number of shards to use for the revision cache
	DefaultRevisionCacheShardCount = 16
)

// RevisionCache is an interface that can be used to fetch a DocumentRevision for a Doc ID and Rev ID pair.
type RevisionCache interface {
	// Get returns the given revision, and stores if not already cached.
	// When includeDelta=true, the returned DocumentRevision will include delta - requires additional locking during retrieval.
	Get(ctx context.Context, docID, revID string, includeDelta bool) (DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached.
	GetActive(ctx context.Context, docID string) (docRev DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(ctx context.Context, docID, revID string) (docRev DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(ctx context.Context, docRev DocumentRevision)

	// Update will remove existing value and re-create new one
	Upsert(ctx context.Context, docRev DocumentRevision)

	// Invalidate marks a revision in the cache as invalid. This is used to call into LoadInvalidRevFromBackingStore in LRU.
	// Marked revision denotes that this value should not be used and should be replaced. Used in the event of an user
	// xattr only update where there is no revision change.
	Invalidate(ctx context.Context, docID, revID string)

	// UpdateDelta stores the given toDelta value in the given rev if cached
	UpdateDelta(ctx context.Context, docID, revID string, toDelta RevisionDelta)
}

const (
	RevCacheIncludeDelta = true
	RevCacheOmitDelta    = false
)

// Force compile-time check of all RevisionCache types for interface
var _ RevisionCache = &LRURevisionCache{}
var _ RevisionCache = &ShardedLRURevisionCache{}
var _ RevisionCache = &BypassRevisionCache{}

// NewRevisionCache returns a RevisionCache implementation for the given config options.
func NewRevisionCache(cacheOptions *RevisionCacheOptions, backingStore RevisionCacheBackingStore, cacheStats *base.CacheStats) RevisionCache {

	// If cacheOptions is not passed in, use defaults
	if cacheOptions == nil {
		cacheOptions = DefaultRevisionCacheOptions()
	}

	if cacheOptions.Size == 0 {
		bypassStat := cacheStats.RevisionCacheBypass
		return NewBypassRevisionCache(backingStore, bypassStat)
	}

	cacheHitStat := cacheStats.RevisionCacheHits
	cacheMissStat := cacheStats.RevisionCacheMisses

	if cacheOptions.ShardCount > 1 {
		return NewShardedLRURevisionCache(cacheOptions.ShardCount, cacheOptions.Size, backingStore, cacheHitStat, cacheMissStat)
	}

	return NewLRURevisionCache(cacheOptions.Size, backingStore, cacheHitStat, cacheMissStat)
}

type RevisionCacheOptions struct {
	Size       uint32
	ShardCount uint16
}

func DefaultRevisionCacheOptions() *RevisionCacheOptions {
	return &RevisionCacheOptions{
		Size:       DefaultRevisionCacheSize,
		ShardCount: DefaultRevisionCacheShardCount,
	}
}

// RevisionCacheBackingStore is the interface required to be passed into a RevisionCache constructor to provide a backing store for loading documents.
type RevisionCacheBackingStore interface {
	GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error)
	GetRevision(ctx context.Context, doc *Document, revid string) ([]byte, AttachmentsMeta, error)
}
type IDAndRev struct {
	DocID string
	RevID string
}

// RevisionDelta stores data about a delta between a revision and ToRevID.
type RevisionDelta struct {
	ToRevID               string                  // Target revID for the delta
	DeltaBytes            []byte                  // The actual delta
	AttachmentStorageMeta []AttachmentStorageMeta // Storage metadata of all attachments present on ToRevID
	ToChannels            base.Set                // Full list of channels for the to revision
	RevisionHistory       []string                // Revision history from parent of ToRevID to source revID, in descending order
	ToDeleted             bool                    // Flag if ToRevID is a tombstone
}

func NewRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision, deleted bool, toRevAttStorageMeta []AttachmentStorageMeta) RevisionDelta {
	return RevisionDelta{
		ToRevID:               toRevision.RevID,
		DeltaBytes:            deltaBytes,
		AttachmentStorageMeta: toRevAttStorageMeta,
		ToChannels:            toRevision.Channels,
		RevisionHistory:       toRevision.History.ParseAncestorRevisions(fromRevID),
		ToDeleted:             deleted,
	}
}

// This is the RevisionCacheLoaderFunc callback for the context's RevisionCache.
// Its job is to load a revision from the bucket when there's a cache miss.
func revCacheLoader(ctx context.Context, backingStore RevisionCacheBackingStore, id IDAndRev) (bodyBytes []byte, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, err error) {
	var doc *Document
	if doc, err = backingStore.GetDocument(ctx, id.DocID, DocUnmarshalSync); doc == nil {
		return bodyBytes, history, channels, removed, attachments, deleted, expiry, err
	}

	return revCacheLoaderForDocument(ctx, backingStore, doc, id.RevID)
}

// Common revCacheLoader functionality used either during a cache miss (from revCacheLoader), or directly when retrieving current rev from cache
func revCacheLoaderForDocument(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document, revid string) (bodyBytes []byte, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, err error) {
	if bodyBytes, attachments, err = backingStore.GetRevision(ctx, doc, revid); err != nil {
		// If we can't find the revision (either as active or conflicted body from the document, or as old revision body backup), check whether
		// the revision was a channel removal. If so, we want to store as removal in the revision cache
		removalBodyBytes, removalHistory, activeChannels, isRemoval, isDelete, isRemovalErr := doc.IsChannelRemoval(revid)
		if isRemovalErr != nil {
			return bodyBytes, history, channels, isRemoval, nil, isDelete, nil, isRemovalErr
		}

		if isRemoval {
			return removalBodyBytes, removalHistory, activeChannels, isRemoval, nil, isDelete, nil, nil
		} else {
			// If this wasn't a removal, return the original error from getRevision
			return bodyBytes, history, channels, removed, nil, isDelete, nil, err
		}
	}
	deleted = doc.History[revid].Deleted

	validatedHistory, getHistoryErr := doc.History.GetHistory(revid)
	if getHistoryErr != nil {
		return bodyBytes, history, channels, removed, nil, deleted, nil, getHistoryErr
	}
	history = EncodeRevisions(doc.ID, validatedHistory)
	channels = doc.History[revid].Channels

	return bodyBytes, history, channels, removed, attachments, deleted, doc.Expiry, err
}
