package bitcaskgo

import "encoding/binary"

// This file provides encode/decode functions for serialisation and deserialisation
// operations. These format methods are generic and does not have any disk or
// memory specific code.
//
// This file has two functions which help us with serialisation of data:
//
//	encodeKV - takes the key value pair and encodes them into bytes
//	decodeKV - takes a bunch of bytes and decodes them into key value pairs
//
// headerSize specifies the total header size. Our key-value pair, when stored on
// disk looks like this:
//
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ timestamp │ key_size │ value_size │ key │ value │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//
// This is analogous to a typical database's row (or a record). The total length of
// the row is variable, depending on the contents of the key and value.
//
// The first three fields form the header:
//
//	┌─────────────────────┬────────────────────┬──────────────────────┐
//	│ timestamp (4 bytes) │ key_size (4 bytes) │ value_size (4 bytes) │
//	└─────────────────────┴────────────────────┴──────────────────────┘
//
// These three fields store unsigned integers of size 4 bytes, giving our header a
// fixed length of 12 bytes.
//
// The timestamp field stores the time when the record was inserted, in unix epoch seconds.
//
// The key_size and value_size fields store the length of bytes occupied by the key and value.
//
// The maximum integer stored by 4 bytes is 4,294,967,295 (2 ** 32 - 1), roughly 4.2GB.
// So, the size of each key or value cannot exceed this. Theoretically, a single row
// can be as large as ~8.4GB.
const headerSize = 12

// KeyEntry holds the key-value metadata, in the following format:
//
//	┌──────────────────────┬─────────────────────┬─────────────────────┐
//	│ entry_size (8 bytes) │ entry_pos (8 bytes) │ timestamp (4 bytes) │
//	└──────────────────────┴─────────────────────┴─────────────────────┘
//
// Whenever we insert/update a key, we create a new KeyEntry object and
// insert that into `keyDir`, which is a hash table that maps every key to
// the above fixed-size metadata.
type KeyEntry struct {
	entrySize uint
	entryPos  int64
	timestamp uint32
}

func NewKeyEntry(entrySize uint, entryPos int64, timestamp uint32) KeyEntry {
	return KeyEntry{entrySize, entryPos, timestamp}
}

func encodeHeader(timestamp uint32, keySize uint32, valueSize uint32) []byte {
	// todo: make stuff like this more efficient? (do benchmarking too)
	// https://stackoverflow.com/a/58776568
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:], timestamp)
	binary.BigEndian.PutUint32(header[headerSize/3:], keySize)
	binary.BigEndian.PutUint32(header[(headerSize/3)*2:], valueSize)
	return header
}

func decodeHeader(header []byte) (uint32, uint32, uint32) {
	timestamp := binary.BigEndian.Uint32(header[0:])
	keySize := binary.BigEndian.Uint32(header[headerSize/3:])
	valueSize := binary.BigEndian.Uint32(header[(headerSize/3)*2:])
	return timestamp, keySize, valueSize
}

func encodeKV(timestamp uint32, key string, value string) (uint, []byte) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	keyBytes := make([]byte, keySize)
	valueBytes := make([]byte, valueSize)

	size := headerSize + uint(keySize) + uint(valueSize)
	keyEnd := headerSize + keySize
	valueEnd := keyEnd + valueSize

	header := encodeHeader(timestamp, keySize, valueSize)
	copy(keyBytes, key)
	copy(valueBytes, value)

	kv := make([]byte, size)
	copy(kv[0:headerSize], header)
	copy(kv[headerSize:keyEnd], keyBytes)
	copy(kv[keyEnd:valueEnd], valueBytes)

	return size, kv
}

func decodeKV(data []byte) (uint32, string, string) {
	timestamp, keySize, _ := decodeHeader(data)

	keyOffset := headerSize
	valueOffset := headerSize + keySize

	key := string(data[keyOffset:valueOffset])
	value := string(data[valueOffset:])

	return timestamp, key, value
}

func decodeKVNoHeader(data []byte, keySize uint32) (string, string) {
	key := string(data[:keySize])
	value := string(data[keySize:])
	return key, value
}
