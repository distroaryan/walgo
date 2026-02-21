package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

const numEntries = 10_000_000 // adjustable parameter for the number of entries

// BenchmarkWriteThroughPut measures the throughput for writing operations
func BenchmarkWriteThroughPut(b *testing.B) {
	directory := "BenchMark_Write"
	defer os.RemoveAll(directory)
	wal, err := OpenWAL(directory, false, maxFileSize, maxSegments)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := range numEntries {
			record := Record{
				Key:   fmt.Sprintf("key%d", j),
				Value: fmt.Sprintf("value%d", j),
				Op:    InsertOperation,
			}

			marshalledRecord, err := json.Marshal(record)
			if err != nil {
				b.Errorf("Error marshalling records: %v", err)
			}

			err = wal.WriteEntry(marshalledRecord)

			if err != nil {
				b.Errorf("Error writing entry to the wal: %v", err)
			}
		}
		duration := time.Since(start)

		// ThroughPut : number of entries / total time (in seconds)
		throughPut := float64(numEntries) / duration.Seconds()
		fmt.Printf("WriteThroughPut: %f entries/sec\n", throughPut)
	}
}

// BenchmarkReadThroughPut measures the throughput for reading operations
func BenchmarkReadThroughPut(b *testing.B) {
	directory := "Benchmark_Read"
	defer os.RemoveAll(directory)

	wal, err := OpenWAL(directory, false, maxFileSize, maxSegments)
	if err != nil {
		b.Fatal(ErrorInitialisingWAL)
	}

	for idx := range numEntries {
		record := Record{
			Key:   fmt.Sprintf("key%d", idx),
			Value: fmt.Sprintf("value%d", idx),
			Op:    DeleteOperation,
		}
		marshalledRecord, err := json.Marshal(record)
		if err != nil {
			b.Error(ErrorMarshallingRecord)
		}

		err = wal.WriteEntry(marshalledRecord)
		if err != nil {
			b.Error(ErrorWritingEntryToWAL)
		}
	}

	err = wal.Close()
	if err != nil {
		b.Error(ErrorClosingWAL)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := wal.ReadCurrentFileLogs(); err != nil {
			b.Error("Recovery err: ", err)
			return
		}
		duration := time.Since(start)
		// Calculating the throughput: number of entries / total time (in seconds)
		throughput := float64(numEntries) / duration.Seconds()
		fmt.Printf("ReadThroughput: %f entries/sec\n", throughput)
	}
}

func BenchmarkConcurrentWriteThroughPut(b *testing.B) {
	directory := "BenchMark_ConcurrentWrite"
	defer os.RemoveAll(directory)

	wal, err := OpenWAL(directory, false, maxFileSize, maxSegments)
	if err != nil {
		b.Fatal(ErrorInitialisingWAL)
	}

	// Each go routine performs 10,000 entries and we spin up 100 go routines
	totalEntries := 100 * 10000
	var wg sync.WaitGroup

	
	for b.Loop() {
		start := time.Now()
		// spin up 100 go routines
		for range 100 {		
			// each go routine performs 10,000 entries
			wg.Go(func() {
				for k := range 10000 {
					record := Record{
						Key:   fmt.Sprintf("key%d", k),
						Value: fmt.Sprintf("value%d", k),
						Op:    DeleteOperation,
					}
					marshalledRecord, err := json.Marshal(record)
					if err != nil {
						b.Error(ErrorMarshallingRecord)
					}

					err = wal.WriteEntry(marshalledRecord)
					if err != nil {
						b.Error(ErrorWritingEntryToWAL)
					}
				}
			})
		}
		wg.Wait()
		duration := time.Since(start)
		throughput := float64(totalEntries) / duration.Seconds()
		fmt.Printf("ConcurrentWriteThroughPut: %f entries/sec\n", throughput)
	}
}
