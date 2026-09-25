package storage

import (
	"fmt"
	"testing"
)

func TestGetAcrossLevelsAndTombstones(t *testing.T) {
	s := New(2, 10)
	s.Put("a", "1")
	s.Put("b", "1")
	s.Flush(1)
	s.Put("a", "2")
	s.Delete("b")
	s.Flush(2)
	s.Put("c", "3")

	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Fatalf("a = %q, %v; want 2", v, ok)
	}
	if _, ok := s.Get("b"); ok {
		t.Fatal("b should be deleted by the tombstone in a newer level")
	}
	if v, ok := s.Get("c"); !ok || v != "3" {
		t.Fatalf("c = %q, %v; want 3", v, ok)
	}
}

func TestViewIsImmutable(t *testing.T) {
	s := New(1000, 10)
	for i := 0; i < 100; i++ {
		s.Put(fmt.Sprint(i), "old")
	}
	view := s.Flush(100)

	// Writes and a compaction after the snapshot must not leak into it.
	for i := 0; i < 100; i++ {
		s.Put(fmt.Sprint(i), "new")
	}
	s.Delete("5")
	s.Flush(200)
	s.Compact()

	data := view.Materialize(nil)
	if len(data) != 100 {
		t.Fatalf("snapshot has %d keys, want 100", len(data))
	}
	for k, v := range data {
		if v != "old" {
			t.Fatalf("snapshot key %s = %q, want old", k, v)
		}
	}
	if _, ok := s.Get("5"); ok {
		t.Fatal("5 should be deleted in the live store")
	}
}

func TestCompactionKeepsContent(t *testing.T) {
	s := New(3, 2)
	want := make(map[string]string)
	for i := 0; i < 50; i++ {
		k := fmt.Sprint(i % 17)
		if i%5 == 0 {
			s.Delete(k)
			delete(want, k)
		} else {
			v := fmt.Sprint(i)
			s.Put(k, v)
			want[k] = v
		}
		if s.ShouldFlush() {
			s.Flush(int64(i))
		}
	}
	s.Flush(100)
	s.Compact()

	if _, levels := s.Stats(); levels != 1 {
		t.Fatalf("levels after compaction = %d, want 1", levels)
	}
	got := s.Flush(101).Materialize(nil)
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestMaterializeFilter(t *testing.T) {
	s := New(10, 10)
	s.Put("keep-1", "x")
	s.Put("drop-1", "x")
	data := s.Flush(1).Materialize(func(k string) bool { return k[:4] == "keep" })
	if len(data) != 1 || data["keep-1"] != "x" {
		t.Fatalf("filtered snapshot = %v", data)
	}
}
