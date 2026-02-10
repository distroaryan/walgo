package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	// "strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	maxSegments = 5
	maxFileSize = 64 * 1024 * 1024 // 64 MB
)

const (
	KeyDidNotMatch           = "Recovered entry did not matched the written (key)"
	ValueDidNotMatch         = "Recovered value did not matched the written (value)"
	OperationDidNotMatch     = "Recovered operation did not matched the written (op)"
	ErrorInitialisingWAL     = "Failed to initialise or open WAL"
	ErrorMarshallingRecord   = "Failed to marshal the record or entry"
	ErrorWritingEntryToWAL   = "Failed to write entry to the wal"
	ErrorClosingWAL          = "Failed to close or sync the wal"
	ErrorReadingFromWAL      = "Error reading the entries from wal"
	ErrorUnexpectedFileFound = "Error unexpected file found"
)

// Test to verify that simple read and write operations are working
// without fail
func TestWAL_WriteAndRead(t *testing.T) {
	// t.Parallel()
	// setup: create a temp directory for WAL
	dirPath := "TestWAL_WriteAndRead"
	defer os.RemoveAll(dirPath)

	wal, err := OpenWAL(dirPath, false, maxFileSize, maxSegments)
	require.NoError(t, err, ErrorInitialisingWAL)

	// Test Data
	entries := []Record{
		{Key: "key1", Value: "value1", Op: InsertOperation},
		{Key: "key2", Value: "value2", Op: InsertOperation},
		{Key: "key3", Value: "value3", Op: DeleteOperation},
		{Key: "key4", Value: "value4", Op: DeleteOperation},
	}

	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, ErrorMarshallingRecord)
		err = wal.WriteEntry(marshaledEntry)
		assert.NoError(t, err, ErrorWritingEntryToWAL)
	}

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	recoveredEntries, err := wal.ReadCurrentFileLogs()
	assert.NoError(t, err, ErrorReadingFromWAL)

	// check if recovered entries matches the written entries
	assertCollectionsAreIdentical(t, entries, recoveredEntries)
}

// Test to verify that write and read ops are performed correctly
// after reopening the wal
func TestWAL_WriteAndReadExistingSegment(t *testing.T) {
	directory := "TestWAL_WriteAndReadExistingSegment"
	defer os.RemoveAll(directory)

	wal, err := OpenWAL(directory, false, maxFileSize, maxSegments)
	require.NoError(t, err, ErrorInitialisingWAL)

	entries := generateTestRecords(6)

	// write first 3 entries in wal
	for i := 0; i < 3; i++ {
		marshalledEntry, err := json.Marshal(entries[i])
		assert.NoError(t, err, ErrorMarshallingRecord)
		err = wal.WriteEntry(marshalledEntry)
		assert.NoError(t, err, ErrorWritingEntryToWAL)
	}

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	wal, err = OpenWAL(directory, false, maxFileSize, maxSegments)
	require.NoError(t, err, ErrorInitialisingWAL)

	for i := 3; i < 6; i++ {
		marshalledEntry, err := json.Marshal(entries[i])
		assert.NoError(t, err, ErrorMarshallingRecord)

		err = wal.WriteEntry(marshalledEntry)
		assert.NoError(t, err, ErrorWritingEntryToWAL)
	}

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	recoveredEntries, err := wal.ReadCurrentFileLogs()
	assert.NoError(t, err, ErrorReadingFromWAL)

	assertCollectionsAreIdentical(t, entries, recoveredEntries)
}

// Test to verify that segments are successfully rotated after exceeding maxSize
func TestWAL_Rotation(t *testing.T) {
	directory := "TestWAL_Rotation"
	defer os.RemoveAll(directory)

	smallMaxFileSize := 10 * 1024 // 10kb for testing rotation
	wal, err := OpenWAL(directory, false, int64(smallMaxFileSize), maxSegments)
	require.NoError(t, err, ErrorInitialisingWAL)

	largeValue := strings.Repeat("a", 12*1024) // 12kb exceeds maxFileSize

	records := []Record{
		{Key: "key1", Value: largeValue, Op: InsertOperation},
		{Key: "key2", Value: largeValue, Op: DeleteOperation},
		{Key: "key3", Value: largeValue, Op: InsertOperation},
		{Key: "key4", Value: largeValue, Op: DeleteOperation},
	}

	for _, rec := range records {
		marshalledRecord, err := json.Marshal(rec)
		assert.NoError(t, err, ErrorMarshallingRecord)

		err = wal.WriteEntry(marshalledRecord)
		assert.NoError(t, err, ErrorWritingEntryToWAL)
	}

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	files, err := os.ReadDir(directory)
	require.NoError(t, err, "should be able to read the files without fail")
	assert.Equal(t, 4, len(files), "Expected 3 files")

	// additional check: check the naming of the files, must be in correct order
	for idx, file := range files {
		filename := file.Name()
		expectedFileName := fmt.Sprintf("%s%d", SegmentPrefix, idx)
		assert.Equal(t, expectedFileName, filename, ErrorUnexpectedFileFound)
	}
}

// Test to verify the segments are successfully deleted after exceeding maxSegmentSize
func TestWAL_OldestSegmentDeletion(t *testing.T) {
	directory := "TestWAL_OldestSegmentDeletion"
	defer os.RemoveAll(directory)

	smallMaxFileSize := 10 * 1024 // 10 kb small size for rotation and creation of new segments
	smallSegmentSize := 2
	wal, err := OpenWAL(directory, false, int64(smallMaxFileSize), smallSegmentSize)
	require.NoError(t, err, ErrorInitialisingWAL)

	largeValue := strings.Repeat("a", 12*1024) // 12kb
	records := []Record{
		{Key: "key1", Value: largeValue, Op: InsertOperation},
		{Key: "key2", Value: largeValue, Op: DeleteOperation},
		{Key: "key3", Value: largeValue, Op: InsertOperation},
		{Key: "key4", Value: largeValue, Op: DeleteOperation},
	}

	for _, rec := range records {
		marshalledRecord, err := json.Marshal(rec)
		assert.NoError(t, err, ErrorMarshallingRecord)

		err = wal.WriteEntry(marshalledRecord)
		assert.NoError(t, err, ErrorWritingEntryToWAL)
	}

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	files, err := os.ReadDir(directory)
	require.NoError(t, err, "should be able to read the files without fail")
	assert.Equal(t, 2, len(files), "Expected 3 files")

	for idx, file := range files {
		filename := file.Name()
		expectedFileName := fmt.Sprintf("%s%d", SegmentPrefix, idx+2)
		assert.Equal(t, expectedFileName, filename, ErrorUnexpectedFileFound)
	}
}

// Test to verify the previosuly created files are readable
func TestWAL_ReadFromOffsetIndex(t *testing.T) {
	directory := "TestWAL_ReadFromOffsetIndex"
	defer os.RemoveAll(directory)

	smallMaxFileSize := 10 * 1024 // small file size to create multiple files
	wal, err := OpenWAL(directory, false, int64(smallMaxFileSize), maxSegments)
	require.NoError(t, err, ErrorInitialisingWAL)

	records := generateLargeTestRecords(4)

	for _, rec := range records {
		marshalledRecord, err := json.Marshal(rec)
		assert.NoError(t, err, ErrorMarshallingRecord)

		err = wal.WriteEntry(marshalledRecord)
		assert.NoError(t, err, ErrorWritingEntryToWAL)
	}

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	recoveredRecords, err := wal.ReadAllLogsFromOffset(0)
	assert.NoError(t, err, ErrorReadingFromWAL)
	assertCollectionsAreIdentical(t, records, recoveredRecords)
}

// Test concurrent read and write
func TestWAL_ConcurrentReadAndWrite(t *testing.T) {
	directory := "TestWaL_ConcurrentReadAndWrite"
	defer os.RemoveAll(directory)

	wal, err := OpenWAL(directory, false, maxFileSize, maxSegments)
	assert.NoError(t, err, ErrorInitialisingWAL)

	numRecords := 100
	records := generateTestRecords(numRecords)
	var wg sync.WaitGroup
	wg.Add(numRecords)

	for _, rec := range records {
		marshalledRecord, err := json.Marshal(rec)
		assert.NoError(t, err, ErrorMarshallingRecord)

		go func () {
			defer wg.Done()
			err := wal.WriteEntry(marshalledRecord)
			assert.NoError(t, err, ErrorWritingEntryToWAL)
		}()
	}

	wg.Wait()

	err = wal.Close()
	assert.NoError(t, err, ErrorClosingWAL)

	recoveredEntries, err := wal.ReadCurrentFileLogs()
	assert.NoError(t, err, ErrorReadingFromWAL)

	// the order is not preserved during concurrent, there's a chance 100th entry 
	// was written first and 0th entry was written last

	// the check we can do is make sure the entry key-d, value-d are present
	// the d value should be [0, numRecords]

	keyArray := make([]bool, numRecords)
	valueArray := make([]bool, numRecords)
	for _, rec := range recoveredEntries {
		unmarshalledRecord := Record{}
		err := json.Unmarshal(rec.data, &unmarshalledRecord)
		assert.NoError(t, err, ErrorMarshallingRecord)

		key, value := unmarshalledRecord.Key, unmarshalledRecord.Value
		keyIdx, err := strconv.Atoi(strings.TrimPrefix(key, "key"))
		assert.NoError(t, err, "Error extracting key index")

		valueIdx, err := strconv.Atoi(strings.TrimPrefix(value, "value"))
		assert.NoError(t, err, "Error extracting value index")

		keyArray[keyIdx] = true
		valueArray[valueIdx] = true 
	}

	for idx := range numRecords {
		if !keyArray[idx] || !valueArray[idx] {
			t.Errorf("%d value missing in either key or value", idx)
		}
	}
}

func assertCollectionsAreIdentical(t *testing.T, entries []Record, recoveredEntries []*WAL_Record) {
	t.Helper()
	assert.Equal(t, len(entries), len(recoveredEntries), "Failed too read all the recods")

	for recordIdx, rec := range recoveredEntries {
		unmarshalledRecord := Record{}
		err := json.Unmarshal(rec.data, &unmarshalledRecord)
		assert.NoError(t, err, ErrorMarshallingRecord)

		// matching the entries one by one for clear error statement
		assert.Equal(t, entries[recordIdx].Key, unmarshalledRecord.Key, KeyDidNotMatch)
		assert.Equal(t, entries[recordIdx].Value, unmarshalledRecord.Value, ValueDidNotMatch)
		assert.Equal(t, entries[recordIdx].Op, unmarshalledRecord.Op, OperationDidNotMatch)
	}
}

// for generating test dummy data
func generateTestRecords(length int) []Record {
	records := make([]Record, length)
	var ops = [2]Opbyte{InsertOperation, DeleteOperation} // for assigning alternate operations
	for idx := range length {
		records[idx] = Record{
			Key:   fmt.Sprintf("key%d", idx),
			Value: fmt.Sprintf("value%d", idx),
			Op:    ops[idx%2],
		}
	}
	return records
}

// for generating large test dummy data
func generateLargeTestRecords(length int) []Record {
	largeValue := strings.Repeat("a", 12*1024)
	records := make([]Record, length)
	var ops = [2]Opbyte{InsertOperation, DeleteOperation}

	for idx := range length {
		records[idx] = Record{
			Key: fmt.Sprintf("key%d",idx),
			Value: largeValue,
			Op: ops[idx%2],
		}
	}
	return records
}
