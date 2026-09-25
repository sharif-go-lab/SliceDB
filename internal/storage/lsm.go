package storage

import (
	"sync"
)

// Store is a small in-memory LSM. Writes land in a mutable memtable; once it grows past
// flushSize (or a snapshot is requested) it is frozen into an immutable level tagged with the
// WAL checkpoint it corresponds to, and a fresh memtable takes new writes. Levels are never
// modified after they are created, so a snapshot is just a list of level pointers that can be
// copied out while writes keep going. Old levels are merged in the background (compaction).
// Nothing is ever written to disk.
type Store struct {
	mu        sync.RWMutex
	active    map[string]entry
	levels    []*level // oldest first; the slice is copy-on-write
	flushSize int
	maxLevels int
	resets    uint64 // bumped by Reset/Load so a running compaction can tell its input is gone

	compactMu sync.Mutex
}

type entry struct {
	value   string
	deleted bool
}

type level struct {
	data       map[string]entry
	checkpoint int64
}

// View is an immutable picture of the store as of Checkpoint.
type View struct {
	levels     []*level
	Checkpoint int64
}

func New(flushSize, maxLevels int) *Store {
	if flushSize <= 0 {
		flushSize = 4096
	}
	if maxLevels <= 0 {
		maxLevels = 4
	}
	return &Store{
		active:    make(map[string]entry),
		flushSize: flushSize,
		maxLevels: maxLevels,
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if e, ok := s.active[key]; ok {
		return e.value, !e.deleted
	}
	for i := len(s.levels) - 1; i >= 0; i-- {
		if e, ok := s.levels[i].data[key]; ok {
			return e.value, !e.deleted
		}
	}
	return "", false
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active[key] = entry{value: value}
}

// Delete writes a tombstone so older levels stop answering for key.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active[key] = entry{deleted: true}
}

func (s *Store) ShouldFlush() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.active) >= s.flushSize
}

// Flush freezes the memtable into an immutable level and returns a view of all levels.
func (s *Store) Flush(checkpoint int64) View {
	s.mu.Lock()
	if len(s.active) > 0 {
		levels := make([]*level, len(s.levels), len(s.levels)+1)
		copy(levels, s.levels)
		s.levels = append(levels, &level{data: s.active, checkpoint: checkpoint})
		s.active = make(map[string]entry)
	}
	view := View{levels: s.levels, Checkpoint: checkpoint}
	needCompaction := len(s.levels) > s.maxLevels
	s.mu.Unlock()

	if needCompaction {
		go func() {
			if s.compactMu.TryLock() {
				defer s.compactMu.Unlock()
				s.compactLocked()
			}
		}()
	}
	return view
}

// Compact merges all current levels into one. The merge runs without blocking writes; levels
// flushed meanwhile are kept on top of the merged one. Views taken earlier stay valid because
// the old level maps are left untouched.
func (s *Store) Compact() {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	s.compactLocked()
}

func (s *Store) compactLocked() {
	s.mu.RLock()
	input := s.levels
	resets := s.resets
	s.mu.RUnlock()
	if len(input) < 2 {
		return
	}

	merged := make(map[string]entry)
	for _, l := range input {
		for k, e := range l.data {
			if e.deleted {
				// Nothing lives below the bottom level, so tombstones can go.
				delete(merged, k)
			} else {
				merged[k] = e
			}
		}
	}
	bottom := &level{data: merged, checkpoint: input[len(input)-1].checkpoint}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resets != resets {
		return
	}
	levels := make([]*level, 0, 1+len(s.levels)-len(input))
	levels = append(levels, bottom)
	levels = append(levels, s.levels[len(input):]...)
	s.levels = levels
}

// Load replaces the whole content with data, e.g. a snapshot received from the leader.
func (s *Store) Load(data map[string]string, checkpoint int64) {
	base := make(map[string]entry, len(data))
	for k, v := range data {
		base[k] = entry{value: v}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.resets++
	s.active = make(map[string]entry)
	s.levels = []*level{{data: base, checkpoint: checkpoint}}
}

func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resets++
	s.active = make(map[string]entry)
	s.levels = nil
}

// Stats returns the memtable size and the number of immutable levels.
func (s *Store) Stats() (memtable, levels int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.active), len(s.levels)
}

// Materialize flattens the view into a plain map, keeping only keys accepted by filter (nil = all).
func (v View) Materialize(filter func(string) bool) map[string]string {
	out := make(map[string]string)
	for _, l := range v.levels {
		for k, e := range l.data {
			if filter != nil && !filter(k) {
				continue
			}
			if e.deleted {
				delete(out, k)
			} else {
				out[k] = e.value
			}
		}
	}
	return out
}
