//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

// =====================================================================
// Attachment Compaction Implementation of Background Manager Process
// =====================================================================

type AttachmentCompactionManager struct {
	MarkedAttachments base.AtomicInt
	PurgedAttachments base.AtomicInt
	CompactID         string
	Phase             string
	VBUUIDs           []uint64
	dryRun            bool
	lock              sync.Mutex
}

var _ BackgroundManagerProcessI = &AttachmentCompactionManager{}

func NewAttachmentCompactionManager(metadataStore base.DataStore, metaKeys *base.MetadataKeys) *BackgroundManager {
	return &BackgroundManager{
		name:    "attachment_compaction",
		Process: &AttachmentCompactionManager{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "compact",
		},
		terminator: base.NewSafeTerminator(),
	}
}

func (a *AttachmentCompactionManager) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	database := options["database"].(*Database)
	database.DbStats.Database().CompactionAttachmentStartTime.Set(time.Now().UTC().Unix())

	newRunInit := func() error {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		dryRun, _ := options["dryRun"].(bool)
		if dryRun {
			base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Running as dry run. No attachments will be purged")
		}

		a.dryRun = dryRun
		a.CompactID = uniqueUUID.String()
		base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Starting new compaction run with compact ID: %q", a.CompactID)
		return nil
	}

	if clusterStatus != nil {
		var statusDoc AttachmentManagerStatusDoc
		err := base.JSONUnmarshal(clusterStatus, &statusDoc)

		reset, ok := options["reset"].(bool)
		if reset && ok {
			base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Resetting compaction process. Will not  resume any "+
				"partially completed process")
		}

		// If the previous run completed, or there was an error during unmarshalling the status we will start the
		// process from scratch with a new compaction ID. Otherwise, we should resume with the compact ID, phase and
		// stats specified in the doc.
		if statusDoc.State == BackgroundProcessStateCompleted || err != nil || (reset && ok) {
			return newRunInit()
		} else {
			a.CompactID = statusDoc.CompactID
			a.Phase = statusDoc.Phase
			a.dryRun = statusDoc.DryRun
			a.MarkedAttachments.Set(statusDoc.MarkedAttachments)
			a.PurgedAttachments.Set(statusDoc.PurgedAttachments)
			a.VBUUIDs = statusDoc.VBUUIDs

			base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Attempting to resume compaction with compact ID: %q phase %q", a.CompactID, a.Phase)
		}

		return nil

	}

	return newRunInit()
}

func (a *AttachmentCompactionManager) PurgeDCPMetadata(ctx context.Context, datastore base.DataStore, database *Database, metadataKeyPrefix string) error {

	bucket, err := base.AsGocbV2Bucket(database.Bucket)
	if err != nil {
		return err
	}
	numVbuckets, err := bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	metadata := base.NewDCPMetadataCS(datastore, numVbuckets, base.DefaultNumWorkers, metadataKeyPrefix)
	base.InfofCtx(ctx, base.KeyDCP, "purging persisted dcp metadata for attachment compaction run %s", a.CompactID)
	metadata.Purge(base.DefaultNumWorkers)
	return nil
}

func (a *AttachmentCompactionManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	database := options["database"].(*Database)

	// Attachment compaction only needs to operate the default scope/collection,
	// because all collection-based attachments will be written by 3.1+ and will be stored in the new format that doesn't need compaction.
	//
	// This may not be true if a user migrated/XDCR'd an existing _default collection into a named collection,
	// but we'll consider that a follow-up enhancement to point this compaction operation at arbitrary collections.
	dataStore := database.Bucket.DefaultDataStore()
	collectionID := base.DefaultCollectionID
	var metadataKeyPrefix string

	persistClusterStatus := func() {
		err := persistClusterStatusCallback()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to persist cluster status on-demand following completion of phase: %v", err)
		}
	}

	defer persistClusterStatus()

	var rollbackErr gocbcore.DCPRollbackError

	// Need to check the current phase in the event we are resuming - No need to run mark again if we got as far as
	// cleanup last time...
	var err error
	switch a.Phase {
	case "mark", "":
		a.SetPhase("mark")
		worker := func() (shouldRetry bool, err error, value interface{}) {
			persistClusterStatus()
			_, a.VBUUIDs, metadataKeyPrefix, err = attachmentCompactMarkPhase(ctx, dataStore, collectionID, database, a.CompactID, terminator, &a.MarkedAttachments)
			if err != nil {
				shouldRetry, err = a.handleAttachmentCompactionRollbackError(ctx, options, dataStore, database, err, MarkPhase, metadataKeyPrefix)
			}
			return shouldRetry, err, nil
		}
		// retry loop for handling a rollback during mark phase of compaction process
		err, _ = base.RetryLoop("attachmentCompactMarkPhase", worker, base.CreateMaxDoublingSleeperFunc(25, 100, 10000))
		if err != nil || terminator.IsClosed() {
			if errors.As(err, &rollbackErr) || errors.Is(err, base.ErrVbUUIDMismatch) {
				// log warning to show we hit max number of retries
				base.WarnfCtx(ctx, "maximum retry attempts reached on mark phase: %v", err)
			}
			return err
		}
		fallthrough
	case "sweep":
		a.SetPhase("sweep")
		persistClusterStatus()
		_, err := attachmentCompactSweepPhase(ctx, dataStore, collectionID, database, a.CompactID, a.VBUUIDs, a.dryRun, terminator, &a.PurgedAttachments)
		if err != nil || terminator.IsClosed() {
			return err
		}
		fallthrough
	case "cleanup":
		a.SetPhase("cleanup")
		worker := func() (shouldRetry bool, err error, value interface{}) {
			persistClusterStatus()
			metadataKeyPrefix, err = attachmentCompactCleanupPhase(ctx, dataStore, collectionID, database, a.CompactID, a.VBUUIDs, terminator)
			if err != nil {
				shouldRetry, err = a.handleAttachmentCompactionRollbackError(ctx, options, dataStore, database, err, CleanupPhase, metadataKeyPrefix)
			}
			return shouldRetry, err, nil
		}
		// retry loop for handling a rollback during mark phase of compaction process
		err, _ = base.RetryLoop("attachmentCompactCleanupPhase", worker, base.CreateMaxDoublingSleeperFunc(25, 100, 10000))
		if err != nil || terminator.IsClosed() {
			if errors.As(err, &rollbackErr) || errors.Is(err, base.ErrVbUUIDMismatch) {
				// log warning to show we hit max number of retries
				base.WarnfCtx(ctx, "maximum retry attempts reached on cleanup phase: %v", err)
			}
			return err
		}
	}

	a.SetPhase("")
	return nil
}

func (a *AttachmentCompactionManager) handleAttachmentCompactionRollbackError(ctx context.Context, options map[string]interface{}, dataStore base.DataStore, database *Database, err error, phase, keyPrefix string) (bool, error) {
	var rollbackErr gocbcore.DCPRollbackError
	if errors.As(err, &rollbackErr) || errors.Is(err, base.ErrVbUUIDMismatch) {
		base.InfofCtx(ctx, base.KeyDCP, "rollback indicated on %s phase of attachment compaction, resetting the task", phase)
		// to rollback any phase for attachment compaction we need to purge all persisted dcp metadata
		err = a.PurgeDCPMetadata(ctx, dataStore, database, keyPrefix)
		if err != nil {
			base.WarnfCtx(ctx, "error occurred during purging of dcp metadata: %w", err)
			return false, err
		}
		if phase == MarkPhase {
			// initialise new compaction run as we want to start the phase mark again in event of rollback
			err = a.Init(ctx, options, nil)
			if err != nil {
				base.WarnfCtx(ctx, "error on initialization of new run after rollback has been indicated, %w", err)
				return false, err
			}
		} else {
			// we only handle rollback for mark and cleanup so if we call here it will be for cleanup phase
			// we need to clear the vbUUID's on the manager for cleanup phase otherwise we will end up in loop of constant rollback
			// as these are used for the initial metadata on the client
			a.VBUUIDs = nil
		}
		// we should try again if it is rollback error
		return true, nil
	}
	// if error isn't rollback then assume it's not recoverable
	return false, err
}

func (a *AttachmentCompactionManager) SetPhase(phase string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.Phase = phase
}

type AttachmentManagerResponse struct {
	BackgroundManagerStatus
	MarkedAttachments int64  `json:"marked_attachments"`
	PurgedAttachments int64  `json:"purged_attachments"`
	CompactID         string `json:"compact_id"`
	Phase             string `json:"phase,omitempty"`
	DryRun            bool   `json:"dry_run,omitempty"`
}

type AttachmentManagerMeta struct {
	VBUUIDs []uint64 `json:"vbuuids"`
}

type AttachmentManagerStatusDoc struct {
	AttachmentManagerResponse `json:"status"`
	AttachmentManagerMeta     `json:"meta"`
}

func (a *AttachmentCompactionManager) GetProcessStatus(status BackgroundManagerStatus) ([]byte, []byte, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	response := AttachmentManagerResponse{
		BackgroundManagerStatus: status,
		MarkedAttachments:       a.MarkedAttachments.Value(),
		PurgedAttachments:       a.PurgedAttachments.Value(),
		CompactID:               a.CompactID,
		Phase:                   a.Phase,
		DryRun:                  a.dryRun,
	}

	meta := AttachmentManagerMeta{
		VBUUIDs: a.VBUUIDs,
	}

	statusJSON, err := base.JSONMarshal(response)
	if err != nil {
		return nil, nil, err
	}

	metaJSON, err := base.JSONMarshal(meta)
	if err != nil {
		return nil, nil, err
	}

	return statusJSON, metaJSON, err
}

func (a *AttachmentCompactionManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.MarkedAttachments.Set(0)
	a.PurgedAttachments.Set(0)
	a.dryRun = false
}
