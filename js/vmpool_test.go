package js

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

const kVMPoolTestTimeout = 90 * time.Second

func runSequentially(ctx context.Context, testFunc func(context.Context) bool, numTasks int) time.Duration {
	myCtx, cancel := context.WithTimeout(ctx, kVMPoolTestTimeout)
	defer cancel()
	startTime := time.Now()
	for i := 0; i < numTasks; i++ {
		testFunc(myCtx)
	}
	return time.Since(startTime)
}

func runConcurrently(ctx context.Context, testFunc func(context.Context) bool, numTasks int, numThreads int) time.Duration {
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			myCtx, cancel := context.WithTimeout(ctx, kVMPoolTestTimeout)
			defer cancel()
			for j := 0; j < numTasks/numThreads; j++ {
				testFunc(myCtx)
			}
		}()
	}
	wg.Wait()
	return time.Since(startTime)
}

// Asserts that running testFunc in 100 concurrent goroutines is no more than 10% slower
// than running it 100 times in succession. A low bar indeed, but can detect some serious
// bottlenecks, or of course deadlocks.
func testConcurrently(t *testing.T, ctx context.Context, testFunc func(context.Context) bool) bool {
	const kNumTasks = 65536 * 10
	const kNumThreads = 8

	// prime the pump:
	runSequentially(ctx, testFunc, 1)

	base.WarnfCtx(context.TODO(), "---- Starting sequential tasks ----")
	sequentialDuration := runSequentially(ctx, testFunc, kNumTasks)
	base.WarnfCtx(context.TODO(), "---- Starting concurrent tasks ----")
	concurrentDuration := runConcurrently(ctx, testFunc, kNumTasks, kNumThreads)
	base.WarnfCtx(context.TODO(), "---- End ----")

	log.Printf("---- %d sequential took %v, concurrent (%d threads) took %v ... speedup is %f",
		kNumTasks, sequentialDuration, kNumThreads, concurrentDuration,
		float64(sequentialDuration)/float64(concurrentDuration))
	return assert.LessOrEqual(t, float64(concurrentDuration), 1.1*float64(sequentialDuration))
}

func TestPoolsConcurrently(t *testing.T) {
	log.Printf("FYI, GOMAXPROCS = %d", runtime.GOMAXPROCS(0))
	const kJSCode = `function(n) {return n * n;}`

	ctx := base.TestCtx(t)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)

	t.Run("Function", func(t *testing.T) {
		testConcurrently(t, ctx, func(ctx context.Context) bool {
			result, err := service.Run(ctx, 13)
			return assert.NoError(t, err) && assert.EqualValues(t, result, 169)
		})
	})
}

func BenchmarkVMPoolSequentially(b *testing.B) {
	const kJSCode = `function(n) {return n * n;}`
	ctx := base.TestCtx(b)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, 13)
		return assert.NoError(b, err) && assert.EqualValues(b, result, 169)
	}
	b.ResetTimer()
	runSequentially(ctx, testFunc, b.N)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolConcurrently(b *testing.B) {
	const kNumThreads = 8
	const kJSCode = `function(n) {return n * n;}`
	ctx := base.TestCtx(b)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, 13)
		return assert.NoError(b, err) && assert.EqualValues(b, result, 169)
	}
	b.ResetTimer()
	runConcurrently(ctx, testFunc, b.N, kNumThreads)
	b.StopTimer()
	pool.Close()
}
