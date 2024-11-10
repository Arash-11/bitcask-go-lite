# Bitcask-go

Bitcask-go is a (simpler) implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf), Riak's log-structured hash table for fast key-value data, written in Go.

## Usage
```go
db, _ := NewBitcaskStore("anime.db")
db.Set("title", "One Piece")
animeTitle := db.Get("title")
```
