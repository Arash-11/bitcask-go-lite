package bitcaskgolite

import (
	"os"
	"testing"
)

func TestDiskStore_Get(t *testing.T) {
	store, err := NewBitcaskStore("test.db")
	if err != nil {
		t.Fatalf("failed to create disk store: %v", err)
	}
	defer os.Remove("test.db")
	store.Set("name", "jojo")
	if val := store.Get("name"); val != "jojo" {
		t.Errorf("Get() = %v, want %v", val, "jojo")
	}
}

func TestDiskStore_GetInvalid(t *testing.T) {
	store, err := NewBitcaskStore("test.db")
	if err != nil {
		t.Fatalf("failed to create disk store: %v", err)
	}
	defer os.Remove("test.db")
	if val := store.Get("some key"); val != "" {
		t.Errorf("Get() = %v, want %v", val, "")
	}
}

func TestDiskStore_SetWithPersistence(t *testing.T) {
	store, err := NewBitcaskStore("anime.db")
	if err != nil {
		t.Fatalf("failed to create disk store: %v", err)
	}
	defer os.Remove("anime.db")

	tests := map[string]string{
		"naruto uzumaki": "naruto",
		"itadori":        "jujutsu kaisen",
		"luffy":          "one piece",
		"isagi":          "blue lock",
		"thorfinn":       "vinland saga",
		"avilio":         "91 days",
		"okazaki":        "gto",
	}
	for key, val := range tests {
		store.Set(key, val)
		if store.Get(key) != val {
			t.Errorf("Get() = %v, want %v", store.Get(key), val)
		}
	}
	store.Close()
	store, err = NewBitcaskStore("anime.db")
	if err != nil {
		t.Fatalf("failed to create disk store: %v", err)
	}
	for key, val := range tests {
		if store.Get(key) != val {
			t.Errorf("Get() = %v, want %v", store.Get(key), val)
		}
	}
	store.Close()
}

func TestDiskStore_Delete(t *testing.T) {
	store, err := NewBitcaskStore("anime.db")
	if err != nil {
		t.Fatalf("failed to create disk store: %v", err)
	}
	defer os.Remove("anime.db")

	tests := map[string]string{
		"naruto uzumaki": "naruto",
		"itadori":        "jujutsu kaisen",
		"luffy":          "one piece",
		"isagi":          "blue lock",
		"thorfinn":       "vinland saga",
		"avilio":         "91 days",
		"okazaki":        "gto",
	}
	for key, val := range tests {
		store.Set(key, val)
	}
	for key := range tests {
		store.Set(key, "")
	}
	store.Set("end", "yes")
	store.Close()

	store, err = NewBitcaskStore("anime.db")
	if err != nil {
		t.Fatalf("failed to create disk store: %v", err)
	}
	for key := range tests {
		if store.Get(key) != "" {
			t.Errorf("Get() = %v, want '' (empty)", store.Get(key))
		}
	}
	if store.Get("end") != "yes" {
		t.Errorf("Get() = %v, want %v", store.Get("end"), "yes")
	}
	store.Close()
}
