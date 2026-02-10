package store

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const numEntries = 1_000_000

// BenchMarkWriteThroughPut measures the throughput for writing operations
func BenchmarkWriteThroughPut(b *testing.B) {
	directory := "BenchMark_Write"
	defer os.RemoveAll(directory)

	store, err := InitializeStore(directory)
	if err != nil {
		b.Fatalf("Failed to initialise store: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := range numEntries {
			key := fmt.Sprintf("key%d", j)
			value := fmt.Sprintf("value%d", j)
			err := store.SET(key, value)
			if err != nil {
				b.Errorf("Error setting the fields: %v", err)
			}
		}
		duration := time.Since(start)
		throughPut := float64(numEntries) / duration.Seconds()
		fmt.Printf("WriteThroughPut: %f entries/sec\n", throughPut)
	}
}
