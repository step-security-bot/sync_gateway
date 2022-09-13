package base

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// Fatalf logs and exits.
func (tbp *TestBucketPool) Fatalf(ctx context.Context, format string, args ...interface{}) {
	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	FatalfCtx(ctx, format, args...)
}

// Logf formats the given test bucket logging and logs to stderr.
func (tbp *TestBucketPool) Logf(ctx context.Context, format string, args ...interface{}) {
	if tbp != nil && !tbp.verbose.IsTrue() {
		return
	}

	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	if colorEnabled() {
		// Green
		format = "\033[0;32m" + format + "\033[0m"
	}

	_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
}

// getTestBucketSpec returns a new BucketSpec for the given test bucket name.
func getTestBucketSpec(testBucketName tbpBucketName) BucketSpec {
	bucketSpec := tbpDefaultBucketSpec
	bucketSpec.BucketName = string(testBucketName)
	bucketSpec.TLSSkipVerify = TestTLSSkipVerify()
	return bucketSpec
}

// RequireNumTestBuckets skips the given test if there are not enough test buckets available to use.
func RequireNumTestBuckets(t *testing.T, numRequired int) {
	usable := GTestBucketPool.numUsableBuckets()
	if usable < numRequired {
		t.Skipf("Only had %d usable test buckets available (test requires %d)", usable, numRequired)
	}
}

// numUsableBuckets returns the total number of buckets in the pool that can be used by a test.
func (tbp *TestBucketPool) numUsableBuckets() int {
	if !tbp.integrationMode {
		// we can create virtually endless walrus buckets,
		// so report back 10 to match a fully available CBS bucket pool.
		return 10
	}
	return tbpNumBuckets() - int(atomic.LoadUint32(&tbp.preservedBucketCount))
}
