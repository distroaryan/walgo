package store

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Aryan123-rgb/kv-store/wal"
)

const (
	maxFileSize = 64 * 1024 * 1024
	maxSegments = 5
)

type Store struct {
	db   map[string]string
	lock sync.RWMutex
	wal  *wal.WAL
}

func InitializeStore(directory string) (*Store, error) {
	wallog, err := wal.OpenWAL(directory, false, maxFileSize, maxSegments)
	if err != nil {
		return nil, err
	}
	return &Store{
		db:  make(map[string]string),
		wal: wallog,
	}, nil
}

func (s *Store) GET(key string) (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	value, isExists := s.db[key]
	if !isExists {
		return "", fmt.Errorf("Key not present in db")
	}

	return value, nil
}

func (s *Store) SET(key, value string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// maintain the log
	record := wal.Record{
		Key:   key,
		Value: value,
		Op:    wal.InsertOperation,
	}
	marshalledRecord, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("Failed to marshall record")
	}
	err = s.wal.WriteEntry(marshalledRecord)
	if err != nil {
		return fmt.Errorf("Failed to write entry to WAL: %v", err)
	}

	err = s.wal.Sync()
	if err != nil {
		return fmt.Errorf("Failed to sync the entries")
	}

	// make changes in db
	s.db[key] = value

	return nil
}

func (s *Store) DELETE(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// maintain the log
	record := wal.Record{
		Key:   key,
		Value: "",
		Op:    wal.DeleteOperation,
	}
	marshalledRecord, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("Failed to marshall record")
	}
	err = s.wal.WriteEntry(marshalledRecord)
	if err != nil {
		return fmt.Errorf("Failed to write entry to WAL")
	}
	err = s.wal.Sync()
	if err != nil {
		return fmt.Errorf("Failed to sync the entries")
	}

	// make changes in db
	delete(s.db, key)
	return nil 
}

func (s *Store) Recover() error {
	s.lock.Lock()
	recoveredEntries, err := s.wal.ReadAllLogsFromOffset(0)
	if err != nil {
		return err 
	}
	s.lock.Unlock()
	
	// unmarshall and apply the operations
	for _, walRecords := range recoveredEntries {
		unmarshalledRecord := wal.Record{}
		err := json.Unmarshal(walRecords.Data, &unmarshalledRecord)
		if err != nil {
			return err 
		}

		key, value := unmarshalledRecord.Key, unmarshalledRecord.Value
		op := unmarshalledRecord.Op

		if op == wal.InsertOperation {
			s.SET(key, value)
		} else{
			s.DELETE(key)
		}
	}
	return nil
}