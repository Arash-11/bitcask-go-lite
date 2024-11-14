package bitcaskgolite

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"
)

// BitcaskStore is a Log-Structured Hash Table as described in the Bitcask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's metadata, including location, on
// the disk.
//
// BitcaskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing key-value pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; until it is completed,
// we cannot use the database.
//
// Typical usage example:
//
// ```
//
//	db, _ := NewBitcaskStore("anime.db")
//	db.Set("title", "One Piece")
//	animeTitle := db.Get("title")
//
// ```
type BitcaskStore struct {
	file           *os.File
	keyDir         map[string]KeyEntry
	lastWrittenPos int64
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewBitcaskStore(fileName string) (*BitcaskStore, error) {
	b := &BitcaskStore{}
	b.keyDir = make(map[string]KeyEntry)

	if isFileExists(fileName) {
		b.initKeyDir(fileName)
	}

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	b.file = file
	return b, err
}

func (b *BitcaskStore) Get(key string) string {
	keyEntry, isKeyPresent := b.keyDir[key]
	if !isKeyPresent {
		return ""
	}

	data := make([]byte, keyEntry.entrySize)
	// todo: handle errors
	b.file.ReadAt(data, keyEntry.entryPos)
	_, _, value := decodeKV(data)
	return value
}

func (b *BitcaskStore) Set(key string, value string) {
	timestamp := uint32(time.Now().Unix())
	entrySize, kv := encodeKV(timestamp, key, value)
	// todo: handle errors
	bytesWritten, _ := b.file.Write(kv)

	b.keyDir[key] = NewKeyEntry(entrySize, b.lastWrittenPos, timestamp)
	b.lastWrittenPos += int64(bytesWritten)
}

func (b *BitcaskStore) Close() bool {
	err := b.file.Close()
	if err != nil {
		clear(b.keyDir)
		b.lastWrittenPos = 0
		return true
	}
	return false
}

func (b *BitcaskStore) initKeyDir(fileName string) {
	file, _ := os.Open(fileName)
	defer file.Close()

	header := make([]byte, headerSize)

	for {
		_, headerErr := io.ReadFull(file, header)
		if headerErr != nil {
			break
		}

		timestamp, keySize, valueSize := decodeHeader(header)
		dataSize := uint(keySize + valueSize)

		data := make([]byte, dataSize)
		_, dataErr := io.ReadFull(file, data)
		if dataErr != nil {
			break
		}

		key, _ := decodeKVNoHeader(data, keySize)
		entrySize := headerSize + dataSize

		b.keyDir[key] = NewKeyEntry(entrySize, b.lastWrittenPos, timestamp)
		b.lastWrittenPos += int64(entrySize)
	}
}
