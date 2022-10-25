/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functionsapitest

import (
	"testing"

	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	//Todo: memWatermarkThresholdMB = ?? 
	memWatermarkThresholdMB := uint64(8192)
	db.TestBucketPoolWithIndexes(m, memWatermarkThresholdMB)
}
