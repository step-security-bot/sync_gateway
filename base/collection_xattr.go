// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

var GetSpecXattr = &gocb.GetSpecOptions{IsXattr: true}
var InsertSpecXattr = &gocb.InsertSpecOptions{IsXattr: true}
var UpsertSpecXattr = &gocb.UpsertSpecOptions{IsXattr: true}
var RemoveSpecXattr = &gocb.RemoveSpecOptions{IsXattr: true}
var LookupOptsAccessDeleted *gocb.LookupInOptions

var _ SubdocXattrStore = &Collection{}

// IsSupported is a shim that queries the parent bucket's feature - required for implementing SubdocXattrStore
func (c *Collection) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return c.Bucket.IsSupported(feature)
}

var _ UserXattrStore = &Collection{}

func init() {
	LookupOptsAccessDeleted = &gocb.LookupInOptions{}
	LookupOptsAccessDeleted.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted
}

// GetSpec required for SubdocXattrStore
func (c *Collection) GetSpec() BucketSpec {
	return c.Bucket.Spec
}

// Implementation of the XattrStore interface primarily invokes common wrappers that in turn invoke SDK-specific SubdocXattrStore API
func (c *Collection) WriteCasWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	return WriteCasWithXattr(c, k, xattrKey, exp, cas, opts, v, xv)
}

func (c *Collection) WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v []byte, xv []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) { // If this is a tombstone, we want to delete the document and update the xattr
	return WriteWithXattr(c, k, xattrKey, exp, cas, opts, v, xv, isDelete, deleteBody)
}

func (c *Collection) DeleteWithXattr(k string, xattrKey string) error {
	return DeleteWithXattr(c, k, xattrKey)
}

func (c *Collection) GetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	return c.SubdocGetXattr(k, xattrKey, xv)
}

func (c *Collection) GetSubDocRaw(k string, subdocKey string) ([]byte, uint64, error) {
	return c.SubdocGetRaw(k, subdocKey)
}

func (c *Collection) WriteSubDoc(k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	return c.SubdocWrite(k, subdocKey, cas, value)
}

func (c *Collection) GetWithXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	return c.SubdocGetBodyAndXattr(k, xattrKey, userXattrKey, rv, xv, uxv)
}

func (c *Collection) WriteUpdateWithXattr(k string, xattrKey string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	return WriteUpdateWithXattr(c, k, xattrKey, userXattrKey, exp, opts, previous, callback)
}

func (c *Collection) SetXattr(k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	return SetXattr(c, k, xattrKey, xv)
}

func (c *Collection) RemoveXattr(k string, xattrKey string, cas uint64) (err error) {
	return RemoveXattr(c, k, xattrKey, cas)
}

func (c *Collection) DeleteXattrs(k string, xattrKeys ...string) (err error) {
	return DeleteXattrs(c, k, xattrKeys...)
}

func (c *Collection) UpdateXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}, deleteBody bool, isDelete bool) (casOut uint64, err error) {
	return UpdateTombstoneXattr(c, k, xattrKey, exp, cas, xv, deleteBody, nil)
}

// SubdocGetXattr retrieves the named xattr
// Notes on error handling
//   - gocb v2 returns subdoc errors at the op level, in the ContentAt response
//   - 'successful' error codes, like SucDocSuccessDeleted, aren't returned, and instead just set the internal.Deleted property on the response
func (c *Collection) SubdocGetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ops := []gocb.LookupInSpec{
		gocb.GetSpec(xattrKey, GetSpecXattr),
	}
	res, lookupErr := c.Collection.LookupIn(k, ops, LookupOptsAccessDeleted)
	if lookupErr == nil {
		xattrContErr := res.ContentAt(0, xv)
		// On error here, treat as the xattr wasn't found
		if xattrContErr != nil {
			DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContErr)
			return 0, ErrXattrNotFound
		}
		cas := uint64(res.Cas())
		return cas, nil
	} else if errors.Is(lookupErr, gocbcore.ErrDocumentNotFound) {
		DebugfCtx(context.TODO(), KeyCRUD, "No document found for key=%s", UD(k))
		return 0, ErrNotFound
	} else {
		return 0, lookupErr
	}
}

func (c *Collection) SubdocGetRaw(k string, subdocKey string) ([]byte, uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	var rawValue []byte

	worker := func() (shouldRetry bool, err error, casOut uint64) {
		ops := []gocb.LookupInSpec{
			gocb.GetSpec(subdocKey, &gocb.GetSpecOptions{}),
		}

		res, lookupErr := c.Collection.LookupIn(k, ops, &gocb.LookupInOptions{})
		if lookupErr != nil {
			isRecoverable := c.isRecoverableReadError(lookupErr)
			if isRecoverable {
				return isRecoverable, lookupErr, 0
			}

			if isKVError(lookupErr, memd.StatusKeyNotFound) {
				return false, ErrNotFound, 0
			}

			return false, lookupErr, 0
		}

		err = res.ContentAt(0, &rawValue)
		if err != nil {
			return false, err, 0
		}

		return false, nil, uint64(res.Cas())
	}

	err, casOut := RetryLoopCas("SubdocGetRaw", worker, c.Bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetRaw with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return rawValue, casOut, err
}

func (c *Collection) SubdocWrite(k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	worker := func() (shouldRetry bool, err error, casOut uint64) {
		mutateOps := []gocb.MutateInSpec{
			gocb.UpsertSpec(subdocKey, bytesToRawMessage(value), &gocb.UpsertSpecOptions{CreatePath: true}),
		}

		result, err := c.Collection.MutateIn(k, mutateOps, &gocb.MutateInOptions{
			Cas:           gocb.Cas(cas),
			StoreSemantic: gocb.StoreSemanticsUpsert,
		})
		if err == nil {
			return false, nil, uint64(result.Cas())
		}

		shouldRetry = c.isRecoverableWriteError(err)
		if shouldRetry {
			return shouldRetry, err, 0
		}

		return false, err, 0
	}

	err, casOut := RetryLoopCas("SubdocWrite", worker, c.Bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocWrite with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return casOut, err
}

// SubdocGetBodyAndXattr retrieves the document body and xattr in a single LookupIn subdoc operation.  Does not require both to exist.
func (c *Collection) SubdocGetBodyAndXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	worker := func() (shouldRetry bool, err error, value uint64) {

		c.Bucket.waitForAvailKvOp()
		defer c.Bucket.releaseKvOp()

		// First, attempt to get the document and xattr in one shot.
		ops := []gocb.LookupInSpec{
			gocb.GetSpec(xattrKey, GetSpecXattr),
			gocb.GetSpec("", &gocb.GetSpecOptions{}),
		}
		res, lookupErr := c.Collection.LookupIn(k, ops, LookupOptsAccessDeleted)

		// There are two 'partial success' error codes:
		//   ErrMemdSubDocBadMulti - one of the subdoc operations failed.  Occurs when doc exists but xattr does not
		//   ErrMemdSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr exists but doc is deleted (tombstone)
		switch lookupErr {
		case nil, gocbcore.ErrMemdSubDocBadMulti:
			// Attempt to retrieve the document body, if present
			docContentErr := res.ContentAt(1, rv)
			xattrContentErr := res.ContentAt(0, xv)
			cas = uint64(res.Cas())

			if isKVError(docContentErr, memd.StatusSubDocMultiPathFailureDeleted) && isKVError(xattrContentErr, memd.StatusSubDocMultiPathFailureDeleted) {
				// No doc, no xattr can be treated as NotFound from Sync Gateway's perspective, even if it is a server tombstone, but should return cas
				DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
				return false, ErrNotFound, cas
			}

			if docContentErr != nil {
				DebugfCtx(context.TODO(), KeyCRUD, "No document body found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), docContentErr)
			}
			// Attempt to retrieve the xattr, if present
			if xattrContentErr != nil {
				DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
			}

		case gocbcore.ErrMemdSubDocMultiPathFailureDeleted:
			//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr may exist but doc is deleted (tombstone)
			xattrContentErr := res.ContentAt(0, xv)
			cas = uint64(res.Cas())
			if xattrContentErr != nil {
				// No doc, no xattr means the doc isn't found
				DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
				return false, ErrNotFound, cas
			}
			return false, nil, cas
		default:
			// KeyNotFound is returned as KVError
			if isKVError(lookupErr, memd.StatusKeyNotFound) {
				return false, ErrNotFound, cas
			}
			shouldRetry = c.isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

		// TODO: We may be able to improve in the future by having this secondary op as part of the first. At present
		// there is no support to obtain more than one xattr in a single operation however MB-28041 is filed for this.
		if userXattrKey != "" {
			userXattrCas, userXattrErr := c.SubdocGetXattr(k, userXattrKey, uxv)
			switch pkgerrors.Cause(userXattrErr) {
			case gocb.ErrDocumentNotFound:
				// If key not found it has been deleted in between the first op and this op.
				return false, err, userXattrCas
			case ErrXattrNotFound:
				// Xattr doesn't exist, can skip
			case nil:
				if cas != userXattrCas {
					return true, errors.New("cas mismatch between user xattr and document body"), uint64(0)
				}
			default:
				// Unknown error occurred
				// Shouldn't retry as any recoverable error will have been retried already in SubdocGetXattr
				return false, userXattrErr, uint64(0)
			}
		}
		return false, nil, cas
	}

	// Kick off retry loop
	err, cas = RetryLoopCas("SubdocGetBodyAndXattr", worker, c.Bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetBodyAndXattr %v", UD(k).Redact())
	}

	return cas, err
}

// SubdocInsertXattr inserts a new server tombstone with an associated mobile xattr.  Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) SubdocInsertXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	supportsTombstoneCreation := c.IsSupported(sgbucket.BucketStoreFeatureCreateDeletedWithXattr)

	var docFlags gocb.SubdocDocFlag
	if supportsTombstoneCreation {
		docFlags = gocb.SubdocDocFlagCreateAsDeleted | gocb.SubdocDocFlagAccessDeleted | gocb.SubdocDocFlagAddDoc
	} else {
		docFlags = gocb.SubdocDocFlagMkDoc
	}

	// convert the passed down sg-bucket mutate in spec to gocb format
	mutateOps, err := c.convertSGBucketSpecToGocbSpec(xv, xattrKey, opts)
	if err != nil {
		return 0, err
	}

	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace, // set replace here, as we're explicitly setting SubdocDocFlagMkDoc above if tombstone creation is not supported
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	options.Internal.DocFlags = docFlags
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// SubdocInsertXattr inserts a document and associated mobile xattr in a single mutateIn operation.  Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) SubdocInsertBodyAndXattr(k string, xattrKey string, exp uint32, v interface{}, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	// convert the passed down sg-bucket mutate in spec to gocb format
	mutateOps, err := c.convertSGBucketSpecToGocbSpec(xv, xattrKey, opts)
	if err != nil {
		return 0, err
	}

	// add replace spec needed for this operation to the mutate in spec
	mutateOps = append(mutateOps, gocb.ReplaceSpec("", bytesToRawMessage(v), nil))
	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsInsert,
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil

}

// SubdocInsert performs a subdoc insert operation to the specified path in the document body.
func (c *Collection) SubdocInsert(k string, fieldPath string, cas uint64, value interface{}) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.InsertSpec(fieldPath, value, nil),
	}
	options := &gocb.MutateInOptions{
		Cas: gocb.Cas(cas),
	}
	_, mutateErr := c.Collection.MutateIn(k, mutateOps, options)

	if errors.Is(mutateErr, gocbcore.ErrDocumentNotFound) {
		return ErrNotFound
	}

	if errors.Is(mutateErr, gocbcore.ErrPathExists) {
		return ErrAlreadyExists
	}

	if errors.Is(mutateErr, gocbcore.ErrPathNotFound) {
		return ErrPathNotFound
	}

	return mutateErr

}

// SubdocSetXattr performs a set of the given xattr. Does a straight set with no cas.
func (c *Collection) SubdocSetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsUpsert,
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}

	return uint64(result.Cas()), nil
}

// SubdocUpdateXattr updates the xattr on an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) SubdocUpdateXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	// convert the passed down sg-bucket mutate in spec to gocb format
	mutateOps, err := c.convertSGBucketSpecToGocbSpec(xv, xattrKey, opts)
	if err != nil {
		return 0, err
	}

	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsUpsert,
		Cas:           gocb.Cas(cas),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// SubdocUpdateBodyAndXattr updates the document body and xattr of an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) SubdocUpdateBodyAndXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	// convert the passed down sg-bucket mutate in spec to gocb format
	mutateOps, err := c.convertSGBucketSpecToGocbSpec(xv, xattrKey, opts)
	if err != nil {
		return 0, err
	}

	// add replace spec needed for this operation to the mutate in spec
	mutateOps = append(mutateOps, gocb.ReplaceSpec("", bytesToRawMessage(v), nil))
	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsUpsert,
		Cas:           gocb.Cas(cas),
	}
	fillMutateInOptions(options, opts)
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// SubdocUpdateBodyAndXattr deletes the document body and updates the xattr of an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) SubdocUpdateXattrDeleteBody(k, xattrKey string, exp uint32, cas uint64, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	// convert the passed down sg-bucket mutate in spec to gocb format
	mutateOps, err := c.convertSGBucketSpecToGocbSpec(xv, xattrKey, opts)
	if err != nil {
		return 0, err
	}

	// add replace spec needed for this operation to the mutate in spec
	mutateOps = append(mutateOps, gocb.RemoveSpec("", nil))
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// SubdocDeleteXattr deletes an xattr of an existing document (or document tombstone)
func (c *Collection) SubdocDeleteXattr(k string, xattrKey string, cas uint64) (err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.RemoveSpec(xattrKey, RemoveSpecXattr),
	}
	options := &gocb.MutateInOptions{
		Cas: gocb.Cas(cas),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	_, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	return mutateErr
}

// SubdocDeleteXattrs will delete the supplied xattr keys from a document. Not a cas safe operation.
func (c *Collection) SubdocDeleteXattrs(k string, xattrKeys ...string) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, gocb.RemoveSpec(xattrKey, RemoveSpecXattr))
	}

	_, mutateErr := c.Collection.MutateIn(k, mutateOps, &gocb.MutateInOptions{})

	return mutateErr
}

// SubdocDeleteXattr deletes the document body and associated xattr of an existing document.
func (c *Collection) SubdocDeleteBodyAndXattr(k string, xattrKey string) (err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.RemoveSpec(xattrKey, RemoveSpecXattr),
		gocb.RemoveSpec("", nil),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
	}
	_, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr == nil {
		return nil
	}

	// StatusKeyNotFound returned if document doesn't exist
	if errors.Is(mutateErr, gocbcore.ErrDocumentNotFound) {
		return ErrNotFound
	}

	// StatusSubDocBadMulti returned if xattr doesn't exist
	if isKVError(mutateErr, memd.StatusSubDocBadMulti) {
		return ErrXattrNotFound
	}
	return mutateErr
}

// SubdocDeleteBody deletes the document body of an existing document, and updates cas and crc32c in the associated xattr.
func (c *Collection) SubdocDeleteBody(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	// convert the passed down sg-bucket mutate in spec to gocb format
	mutateOps, err := c.convertSGBucketSpecToGocbSpec(nil, xattrKey, opts)
	if err != nil {
		return 0, err
	}

	// add replace spec needed for this operation to the mutate in spec
	mutateOps = append(mutateOps, gocb.RemoveSpec("", nil))
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// isKVError compares the status code of a gocb KeyValueError to the provided code.  Used for nested subdoc errors
// where gocb doesn't return a typed error for the underlying error.
func isKVError(err error, code memd.StatusCode) bool {

	switch typedErr := err.(type) {
	case gocb.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case *gocb.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case gocbcore.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case *gocbcore.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case gocbcore.SubDocumentError:
		return isKVError(typedErr.InnerError, code)
	case *gocbcore.SubDocumentError:
		return isKVError(typedErr.InnerError, code)
	}

	return false
}

// If v is []byte or *[]byte, converts to json.RawMessage to avoid duplicate marshalling by gocb.
func bytesToRawMessage(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		return json.RawMessage(val)
	case *[]byte:
		return json.RawMessage(*val)
	default:
		return v
	}
}

func (c *Collection) WriteUserXattr(k string, xattrKey string, xattrVal interface{}) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xattrVal), UpsertSpecXattr),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsUpsert,
	}

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) DeleteUserXattr(k string, xattrKey string) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.RemoveSpec(xattrKey, RemoveSpecXattr),
	}
	options := &gocb.MutateInOptions{
		Cas: gocb.Cas(0),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// convertSGBucketSpecToGocbSpec will take sg-bucket mutate in spec and convert it to a gocb mutate in spec to be passed to the SDK
func (c *Collection) convertSGBucketSpecToGocbSpec(xv interface{}, xattrKey string, opts *sgbucket.MutateInOptions) ([]gocb.MutateInSpec, error) {
	var spec []gocb.MutateInSpec
	if opts == nil {
		opts = InitializeMutateInOptions(opts, SyncXattrName)
	}
	if xv != nil {
		spec = append(spec, gocb.UpsertSpec((xattrKey), bytesToRawMessage(xv), UpsertSpecXattr))
	}

	// grab bucket UUID for sourceID macro expansion
	bucketUUID, err := c.Bucket.UUID()
	if err != nil {
		return nil, err
	}
	for _, v := range opts.Spec {
		if v.Value == "src" {
			spec = append(spec, gocb.UpsertSpec(v.Path, bucketUUID, UpsertSpecXattr))
		} else {
			// if value is no `src` then we are inserting a cas value
			spec = append(spec, gocb.UpsertSpec(v.Path, v.Value, UpsertSpecXattr))
		}
	}
	return spec, nil
}

// InitializeMutateInOptions initialises the mutate in option on the sg-bucket library to values that are needed for macro expansion
func InitializeMutateInOptions(opts *sgbucket.MutateInOptions, xattrName string) *sgbucket.MutateInOptions {
	if opts == nil {
		opts = &sgbucket.MutateInOptions{}
	}
	opts.Spec = []sgbucket.MutateInSpec{
		sgbucket.UpsertSpec(xattrCasPath(xattrName), gocb.MutationMacroCAS),
		sgbucket.UpsertSpec(xattrVersionPath(xattrName), gocb.MutationMacroCAS),
		sgbucket.UpsertSpec(xattrSourceIDPath(xattrName), "src"),
		sgbucket.UpsertSpec(xattrCrc32cPath(xattrName), gocb.MutationMacroValueCRC32c),
	}
	return opts
}
