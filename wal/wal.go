package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// We will sync the current active segment after every syncInterval
// every segment will have a name segement-1, segment-2, segment-3
const (
	syncInterval  = 500 * time.Millisecond
	SegmentPrefix = "segment-"
)

// WAL Structure
type WAL struct {
	directory             string
	currentSegmentFile    *os.File
	lock                  sync.Mutex
	lastLogSequenceNumber uint64
	bufferFileWriter      *bufio.Writer
	syncTimer             *time.Timer
	shouldFysnc           bool
	maxFileSize           int64
	maxSegments           int
	currentSegmentIndex   int
	ctx                   context.Context
	cancel                context.CancelFunc
}

// WAL entry -> one entry/record in the segment
type WAL_Record struct {
	logSequenceNumber uint64
	CRC               uint32
	data              []byte
	isCheckPoint      bool
}

// Initialize a new WAL. If the directory does not exist, it will be created.
// If the directory exists, the last log segment will be opened and the last
// log sequence number will be read from it to resume writing in the same segment
func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments int) (*WAL, error) {
	// create the directory
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// Get the list of the log segments files in the directory
	files, err := filepath.Glob(filepath.Join(directory, SegmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentId int
	if len(files) > 0 {
		// logs exists, find the last segment id
		// segment-1, segment-2. we need to find segment-2
		lastSegmentId, err = FindSegmentWithLastIndex(files)
		if err != nil {
			return nil, err
		}
	} else {
		// create a new segment file
		file, err := CreateNewSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	// open the last log segment file
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", SegmentPrefix, lastSegmentId))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_CREATE|os.O_WRONLY, 0655)
	if err != nil {
		return nil, err
	}

	// seek to the end of the file, to resume writing from there
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:          directory,
		currentSegmentFile: file,
		bufferFileWriter:   bufio.NewWriter(file),
		syncTimer:          time.NewTimer(syncInterval),
		maxFileSize:        maxFileSize,
		maxSegments:        maxSegments,
		ctx:                ctx,
		cancel:             cancel,
		shouldFysnc:        enableFsync,
	}

	// if it's a new file then LSN = 0, if not LSN > 0
	if wal.lastLogSequenceNumber, err = FindLastLogSequenceNumber(file); err != nil {
		return nil, err
	}

	go wal.backgroundSync()

	return wal, nil
}

// WriteEntry writes an entry to the WAL
// it only writes to the buffer cache and when we call sync()
// then it gets stored to the kernel memory and if shouldFsync
// flag is enabled then it gets written to the disk immediately
func (wal *WAL) WriteEntry(data []byte) error {
	return wal.writeEntry(data, false)
}

// rotates the log if needed and writes the data to the kernel memory
func (wal *WAL) writeEntry(data []byte, isCheckPoint bool) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	if err := wal.rotateLogIfNeeded(); err != nil {
		return err
	}

	// if isCheckPoint {
	// 	if err := wal.Sync(); err != nil {
	// 		return fmt.Errorf("could not create checkpoint, error while syncing: %v", err)
	// 	}
	// }

	// create the entry
	wal.lastLogSequenceNumber++
	record := &WAL_Record{
		logSequenceNumber: wal.lastLogSequenceNumber,
		data:              data,
		CRC:               GetCRC32Hash(wal.lastLogSequenceNumber, data),
	}

	serializedRecord := SerializeWAL_Record(record)
	size := int32(len(serializedRecord))
	if err := binary.Write(wal.bufferFileWriter, binary.LittleEndian, size); err != nil {
		return err
	}
	// fmt.Println("Raw Data: ", string(data))
	// fmt.Println("Serialized record: ", string(serializedRecord))
	if _, err := wal.bufferFileWriter.Write(serializedRecord); err != nil {
		return err
	}
	return nil
}

// checks if the current buffer cache + file size >= maximum file size
// if yes then syncs the current segment, deletes the oldest segment and
// creates a new segment and sets it as the current segment
func (wal *WAL) rotateLogIfNeeded() error {
	fileInfo, err := wal.currentSegmentFile.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(wal.bufferFileWriter.Size()) >= wal.maxFileSize {
		// sync the current file
		if err := wal.Sync(); err != nil {
			return nil
		}
		// delete the oldest segment file
		if err := wal.deleteOldestSegment(); err != nil {
			return nil
		}

		// close the current file before creating a new one
		// otherwise they will not get deleted by os.RemoveAll()
		err := wal.currentSegmentFile.Close()
		if err != nil {
			return err
		}

		// create a new segment file
		wal.currentSegmentIndex++
		newFile, err := CreateNewSegmentFile(wal.directory, wal.currentSegmentIndex)
		if err != nil {
			return nil
		}

		// mark the new segment as the current active segment file
		wal.currentSegmentFile = newFile
		wal.bufferFileWriter = bufio.NewWriter(newFile)
	}

	return nil
}

// Write's out any data in the in-memory buffer to the segment file (kernel memory)
// flushes the file to the disk if {shouldFsync} is enabled
// it also resets the syncTimer
func (wal *WAL) Sync() error {
	if err := wal.bufferFileWriter.Flush(); err != nil {
		return nil
	}

	if wal.shouldFysnc {
		if err := wal.currentSegmentFile.Sync(); err != nil {
			return nil
		}
	}
	wal.syncTimer.Reset(syncInterval)
	return nil
}

// deletes the oldest segment file {segment-1, segment-2, segment-3}
// deletes segment-1
func (wal *WAL) deleteOldestSegment() error {
	// find the oldest segment
	files, err := filepath.Glob(filepath.Join(wal.directory, SegmentPrefix+"*"))
	if err != nil {
		return nil
	}

	if len(files) >= wal.maxSegments {
		oldestSegmentId, err := FindOldestSegmentFile(files)
		if err != nil {
			return err
		}

		err = os.Remove(oldestSegmentId)
		if err != nil {
			return err
		}
	}
	return nil
}

// read all the log entries from the current active segment/file
func (wal *WAL) ReadCurrentFileLogs() ([]*WAL_Record, error) {
	file, err := os.OpenFile(wal.currentSegmentFile.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error occured while trying to open the file: %v", err)
	}
	defer file.Close()

	records, err := readAllRecordsFromFile(file)
	if err != nil {
		return nil, fmt.Errorf("Error occured while trying to read the logs from the file: %v", err)
	}
	return records, nil
}

// Reads all the log entries after a given offset index. For example
// {segment-0,1,2,3,4} and offset=2 then the logs from segment=2,3,4 are read
func (wal *WAL) ReadAllLogsFromOffset(offset int) ([]*WAL_Record, error) {
	files, err := filepath.Glob(filepath.Join(wal.directory, SegmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var records []*WAL_Record
	for _, file := range files {
		_, filename := filepath.Split(file)
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(filename, SegmentPrefix))

		if segmentIndex < offset {
			continue
		}

		file, err := os.OpenFile(file, os.O_RDONLY, 0644)

		if err != nil {
			return records, err
		}

		records_from_segment, err := readAllRecordsFromFile(file)
		if err != nil {
			return records, err
		}
		records = append(records, records_from_segment...)
	}
	return records, nil
}

// reads all the records from a given file and returns a slice of WAL_Records
func readAllRecordsFromFile(file *os.File) ([]*WAL_Record, error) {
	var records []*WAL_Record
	for {
		var size int32
		// read the starting 4 bytes to get the size of the data
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return records, fmt.Errorf("Error occured while trying to read the size of record: %v", err)
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return records, fmt.Errorf("Error occured due to partial writes or corrupted log entry: %v", err)
		}

		record, err := DeserializeWAL_Record(data)
		if err != nil {
			return records, fmt.Errorf("Error occured while unmarshalling the record: %v", err)
		}

		records = append(records, record)
	}
	return records, nil
}

// background sync function, is called after every syncInterval time
// closes the background go routine using context during shutdown
func (wal *WAL) backgroundSync() {
	for {
		select {
		case <-wal.syncTimer.C:
			wal.lock.Lock()
			err := wal.Sync()
			defer wal.lock.Unlock()

			if err != nil {
				log.Printf("Error while performing sync: %v", err)
			}
		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *WAL) Close() error {
	wal.cancel()
	if err := wal.Sync(); err != nil {
		return err
	}
	return wal.currentSegmentFile.Close()
}
