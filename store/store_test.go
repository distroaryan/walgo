package store

import (
	// "fmt"
	"encoding/json"
	"fmt"
	"path/filepath"

	"os"
	"testing"

	"github.com/Aryan123-rgb/kv-store/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ErrorInitialisingStore   = "Failed to initialise a store"
	ErrorSettingTheValue     = "Error setting the value"
	ErrorGettingTheValue     = "Error getting the value"
	ErrorUnmarshallingRecord = "Failed to unmarshall record value"
	ErrorInvalidValueFound   = "Error invalid value found"
	ErrorRecoveringStore     = "Failed to restore the previous state"
)

func TestStore_SETAndGET(t *testing.T) {
	directory := "TestStore_SETAndGET"

	store, err := InitializeStore(directory)
	require.NoError(t, err, ErrorInitialisingStore)

	numEntries := 100
	for idx := range numEntries {
		key := fmt.Sprintf("key%d", idx)
		value := fmt.Sprintf("value%d", idx)
		err = store.SET(key, value)
		assert.NoError(t, err, ErrorSettingTheValue)
	}

	recoveredEntries, err := store.wal.ReadCurrentFileLogs()
	for idx, walRecord := range recoveredEntries {
		unmarshalledRecord := wal.Record{}
		err := json.Unmarshal(walRecord.Data, &unmarshalledRecord)
		assert.NoError(t, err, ErrorUnmarshallingRecord)

		key := fmt.Sprintf("key%d", idx)
		value := fmt.Sprintf("value%d", idx)
		assert.Equal(t, unmarshalledRecord.Key, key, ErrorInvalidValueFound)
		assert.Equal(t, unmarshalledRecord.Value, value, ErrorInvalidValueFound)
	}

	// cleanup
	cleanup(directory)
}

func TestStore_RecoverWrite(t *testing.T) {
	directory := "TestStore_RecoverWrite"
	defer os.RemoveAll(directory)

	store, err := InitializeStore(directory)
	require.NoError(t, err, ErrorInitialisingStore)

	numEntries := 10
	for idx := range numEntries {
		key := fmt.Sprintf("key%d", idx)
		value := fmt.Sprintf("value%d", idx)
		err = store.SET(key, value)
		assert.NoError(t, err, ErrorSettingTheValue)
	}

	// assume a crash happened and we initiated a new db instance
	store, err = InitializeStore(directory)

	// recover the previous writes
	err = store.Recover()
	assert.NoError(t, err, ErrorRecoveringStore)


	for idx := range numEntries {
		key := fmt.Sprintf("key%d", idx)
		expectedValue := fmt.Sprintf("value%d", idx)

		actualValue, err := store.GET(key)
		assert.NoError(t, err, ErrorGettingTheValue)
		assert.Equal(t, expectedValue, actualValue, ErrorInvalidValueFound)
	}

	cleanup(directory)
}

// cleanup closes all the file handles and removes the directory
func cleanup(directory string) error {
	files, err := filepath.Glob(directory)
	if err != nil{
		return err 
	}

	for _, filepath := range files {
		file, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
		if err != nil {
			return err 
		}
		err = file.Close()
	}
	os.RemoveAll(directory)
	return nil
}