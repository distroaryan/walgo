package store

import (
	"fmt"
	// "os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ErrorInitialisingStore = "Failed to initialise a store"
	ErrorSettingTheValue   = "Error setting the value"
	ErrorGettingTheValue   = "Error getting the value"
)

// func TestStore_SETAndGET(t *testing.T) {
// 	directory := "TestStore_SETAndGET"
// 	defer os.RemoveAll(directory)

// 	store, err := InitializeStore(directory)
// 	require.NoError(t, err, ErrorInitialisingStore)

// 	key, value := "key1", "value1"

// 	err = store.SET(key, value)
// 	assert.NoError(t, err, ErrorSettingTheValue)

// 	val, err := store.GET(key)
// 	assert.NoError(t, err, ErrorGettingTheValue)

// 	assert.Equal(t, value, val, "Unexpected value retrieved")
// }

func TestStore_Recover(t *testing.T) {
	directory := "TestStore_Recover"
	// defer os.RemoveAll(directory)

	store, err := InitializeStore(directory)
	require.NoError(t, err, ErrorInitialisingStore)

	numEntries := 100
	for idx := range numEntries {
		key := fmt.Sprintf("key%d",idx)
		value := fmt.Sprintf("value%d",idx)
		err = store.SET(key, value)
		assert.NoError(t, err, ErrorSettingTheValue)
	}

	recoveredEntries, err := store.wal.ReadAllLogsFromOffset(0)
	fmt.Println(recoveredEntries)

}
