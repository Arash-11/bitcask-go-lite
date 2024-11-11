# Bitcask-go

Bitcask-go is a (simpler) implementation of [Riak's Bitcask](https://riak.com/assets/bitcask-intro.pdf), written in Go.

It is an embedded and persistent key-value store that uses a log-structured hash table.

## Usage
```go
db, _ := NewBitcaskStore("anime.db")
db.Set("title", "One Piece")
animeTitle := db.Get("title")
db.Close()
```

---

Thanks to https://github.com/avinassh/go-caskdb/tree/start-here for being the inspiration for this project and for getting me started.
