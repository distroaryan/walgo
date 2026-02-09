package wal

type Opbyte byte // operation type

const (
	InsertOperation Opbyte = 1
	DeleteOperation Opbyte = 2
)

type Record struct {
	Value string `json:"value"`
	Key   string `json:"key"`
	Op    Opbyte `json:"op"`
}

