package store

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

const numEntries = 10_000_000

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

// BenchMarkReadThroughPut measures the throughput for reading operations
func BenchmarkReadThroughPut(b *testing.B) {
	directory := "BenchMark_Read"
	defer os.RemoveAll(directory)

	store, err := InitializeStore(directory)
	if err != nil {
		b.Fatalf("Failed to initialise store: %v", err)
	}

	for j := range numEntries {
		key := fmt.Sprintf("key%d", j)
		value := fmt.Sprintf("value%d", j)
		err := store.SET(key, value)
		if err != nil {
			b.Errorf("Error setting the fields: %v", err)
		}
	}

	// sync the logs
	err = store.wal.Close()
	if err != nil {
		b.Errorf("Error syncing the wallogs: %v", err)
	}

	b.ResetTimer()
	// bench test of read
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := range numEntries {
			key := fmt.Sprintf("key%d", j)
			expectedValue := fmt.Sprintf("value%d", j)
			actualValue, err := store.GET(key)
			if err != nil {
				b.Errorf("Error getting the value: %v", err)
			}
			if actualValue != expectedValue {
				b.Error("Unwanted value found")
			}
		}
		store.wal.Close()
		duration := time.Since(start)

		throughPut := float64(numEntries) / duration.Seconds()
		fmt.Printf("ReadThroughPut: %f entries/sec\n", throughPut)
	}
}

// BenchmarkConcurrentWrite measures the throughput of concurrent writes
func BenchmarkConcurrentWriteThroughPut(b *testing.B) {
	directory := "BenchmarkConcurrentWriteThroughPut"
	defer os.RemoveAll(directory)

	store, err := InitializeStore(directory)
	if err != nil {
		b.Fatalf("Failed to initialise store: %v", err)
	}

	// will spin up 1000 goroutines and each routine will form 10,000 operations
	
	b.ResetTimer()
	for b.Loop() {
		var wg sync.WaitGroup
		start := time.Now()
		for range 1000 {
			wg.Add(1)
			go func(){
				defer wg.Done()
				for k := range 10000{
					key := fmt.Sprintf("key%d", k)
					value := fmt.Sprintf("value%d", k)
					err := store.SET(key, value)
					if err != nil {
						b.Errorf("Error setting the fields: %v", err)
					}
				}
			}()
		}
		wg.Wait()
		duration := time.Since(start)
		throughPut := float64(numEntries) / duration.Seconds()
		fmt.Printf("ConcurrentWriteThroughPut: %f entries/sec\n", throughPut)
	}
}
