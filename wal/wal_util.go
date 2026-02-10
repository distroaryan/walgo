package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func FindOldestSegmentFile(files []string) (string, error) {
	oldestSegmentId := math.MaxInt64
	var oldestSegmentFile string
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentId, err := strconv.Atoi(strings.TrimPrefix(fileName, SegmentPrefix))
		if err != nil {
			return oldestSegmentFile, err
		}
		if oldestSegmentId > segmentId {
			oldestSegmentId = segmentId
			oldestSegmentFile = file
		}
	}
	return oldestSegmentFile, nil
}

func FindSegmentWithLastIndex(files []string) (int, error) {
	lastSegmentId := math.MinInt64
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentId, err := strconv.Atoi(strings.TrimPrefix(fileName, SegmentPrefix))
		if err != nil {
			return 0, err
		}
		if lastSegmentId < segmentId {
			lastSegmentId = segmentId
		}
	}
	return lastSegmentId, nil
}

func CreateNewSegmentFile(directory string, segmentId int) (*os.File, error) {
	filepath := filepath.Join(directory, fmt.Sprintf("%s%d", SegmentPrefix, segmentId))
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func FindLastLogSequenceNumber(currentSegmentFile *os.File) (uint64, error) {
	// find the last log record
	record, err := findLastLogRecord(currentSegmentFile)
	if err != nil {
		return 0, err
	}

	if record != nil {
		return record.LogSequenceNumber, nil
	}
	return 0, nil
}

// the logs are being stored like this
// [4 byte: size][actual WAL_Record data]
func findLastLogRecord(currentSegmentFile *os.File) (*WAL_Record, error) {
	file, err := os.OpenFile(currentSegmentFile.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	var previousSize int32
	var offset int64
	var record *WAL_Record

	for {
		var size int32
		// read the starting 4 bytes to get the size of the data
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// file is empty, the file does not contain a single log entry
				if offset == 0 {
					return record, nil
				}

				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				// read the last wal record to get the log sequence number
				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				record, err := DeserializeWAL_Record(data)

				if err != nil {
					return nil, err
				}
				return record, nil
			}
			return nil, err
		}

		// offset represents the position where the actual data is
		offset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		previousSize = size

		// skip to the next entry
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}

func verifyCRC(record *WAL_Record) bool {
	return GetCRC32Hash(record.LogSequenceNumber, record.Data) == record.CRC
}

func GetCRC32Hash(logSequenceNumber uint64, data []byte) uint32 {
	// convert uint62 into bytes to record all 8 bytes
	// without this approach we only used the lowest byte

	buff := make([]byte, 8)
	binary.LittleEndian.AppendUint64(buff, logSequenceNumber)
	byteData := append(data, buff...)
	return crc32.ChecksumIEEE(byteData)
}

func SerializeWAL_Record(record *WAL_Record) []byte {
	// LSN (8 bytes) + CRC (4 bytes) + data
	size := 8 + 4 + len(record.Data)
	buff := make([]byte, size)
	offset := 0 // to maintain the buffer position

	// write lsn
	binary.LittleEndian.PutUint64(buff[offset:offset+8], record.LogSequenceNumber)
	offset += 8

	// write crc
	binary.LittleEndian.PutUint32(buff[offset:offset+4], record.CRC)
	offset += 4

	// write data
	copy(buff[offset:offset+len(record.Data)], record.Data)

	return buff
}

func DeserializeWAL_Record(data []byte) (*WAL_Record, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("data too short: expected at least 12 bytes")
	}

	record := &WAL_Record{}
	offset := 0

	// read lsn
	record.LogSequenceNumber = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// read crc32 hash
	record.CRC = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// read the actual data
	record.Data = make([]byte, len(data)-offset)
	copy(record.Data, data[offset:])

	if !verifyCRC(record) {
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}
	return record, nil 
}
