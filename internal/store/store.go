package store

import (
	"context"
	"kvshard/internal/kverrors"
	"kvshard/internal/store/objects"
	"kvshard/internal/store/wal"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Storage is an interface that defines the operations that can be performed on the store.
type Storage interface {
	Get(key string) (*objects.Object, bool, error)
	Put(key string, value *objects.Object) error
	Size() int64
	Flush() error
	Close() error
	Compact() error
	Delete(key string) error
	Exists(key string) bool
}

type Store struct {
	shardID int
	data    *sync.Map
	count   int64
	wal     *wal.WAL
	writeMu sync.Mutex
}

func NewStore(ctx context.Context, shardID int, logger *slog.Logger) (*Store, error) {
	return NewStoreAt(ctx, shardID, "data", logger)
}

func NewStoreAt(ctx context.Context, shardID int, dataDir string, logger *slog.Logger) (*Store, error) {
	return NewStoreAtWithEncoding(ctx, shardID, dataDir, wal.JSONEncoding, logger)
}

func NewStoreAtWithEncoding(ctx context.Context, shardID int, dataDir string, encoding wal.Encoding, logger *slog.Logger) (*Store, error) {
	config := wal.DefaultConfig()
	config.Encoding = encoding
	return NewStoreAtWithWALConfig(ctx, shardID, dataDir, config, logger)
}

func NewStoreAtWithWALConfig(ctx context.Context, shardID int, dataDir string, config wal.Config, logger *slog.Logger) (*Store, error) {
	storeWal, err := wal.NewAtWithConfig(ctx, shardID, dataDir, config, logger)
	if err != nil {
		return nil, err
	}
	s := &Store{
		shardID: shardID,
		data:    &sync.Map{},
		count:   0,
		wal:     storeWal,
	}
	if err := s.restore(time.Now()); err != nil {
		_ = storeWal.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Size() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *Store) Exists(key string) bool {
	_, ok, _ := s.Get(key)
	return ok
}

func (s *Store) Delete(key string) (err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, ok := s.data.Load(key)
	if !ok {
		return kverrors.ErrKeyNotFound
	}
	if current.(*objects.Object).IsExpired(time.Now()) {
		s.data.Delete(key)
		atomic.AddInt64(&s.count, -1)
		return kverrors.ErrKeyNotFound
	}
	if err := s.wal.Append(wal.Entry{Operation: wal.DeleteOperation, Key: key}); err != nil {
		return err
	}
	s.data.Delete(key)
	atomic.AddInt64(&s.count, -1)
	return nil
}

func (s *Store) Put(key string, value *objects.Object) (err error) {
	if key == "" || value == nil {
		return kverrors.ErrInvalidArguments
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.wal.Append(wal.Entry{Operation: wal.PutOperation, Key: key, Value: value.String(), TTL: value.TTL()}); err != nil {
		return err
	}
	_, loaded := s.data.LoadOrStore(key, value)
	if loaded {
		s.data.Store(key, value)
	} else {
		atomic.AddInt64(&s.count, 1)
	}
	return nil
}

func (s *Store) Flush() (err error) {
	return s.wal.Sync()
}

func (s *Store) Close() error { return s.wal.Close() }

func (s *Store) NeedsCompaction() bool { return s.wal.NeedsCompaction(s.Size()) }

// Compact replaces mutation history with one PUT for each currently live key.
func (s *Store) Compact() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now()
	entries := make([]wal.Entry, 0, s.Size())
	s.data.Range(func(key, value any) bool {
		obj := value.(*objects.Object)
		if obj.IsExpired(now) {
			s.data.Delete(key)
			atomic.AddInt64(&s.count, -1)
			return true
		}
		entries = append(entries, wal.Entry{
			Operation: wal.PutOperation,
			Key:       key.(string),
			Value:     obj.String(),
			TTL:       obj.TTL(),
		})
		return true
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return s.wal.Compact(entries)
}

func (s *Store) Get(key string) (value *objects.Object, ok bool, err error) {
	val, ok := s.data.Load(key)
	if !ok {
		err = kverrors.ErrKeyNotFound
		return value, ok, err
	}
	value = val.(*objects.Object)
	if value.IsExpired(time.Now()) {
		s.deleteExpiredKey(key, value)
		return nil, false, kverrors.ErrKeyNotFound
	}
	return value, ok, err
}

func (s *Store) DeleteExpired(now time.Time) int {
	deleted := 0
	s.data.Range(func(key, value any) bool {
		if value.(*objects.Object).IsExpired(now) && s.deleteExpiredKey(key.(string), value.(*objects.Object)) {
			deleted++
		}
		return true
	})
	return deleted
}

func (s *Store) deleteExpiredKey(key string, expected *objects.Object) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, ok := s.data.Load(key)
	if !ok || current != expected {
		return false
	}
	s.data.Delete(key)
	atomic.AddInt64(&s.count, -1)
	return true
}

func (s *Store) restore(now time.Time) error {
	return s.wal.Replay(func(entry wal.Entry) error {
		switch entry.Operation {
		case wal.PutOperation:
			obj := objects.New(entry.Key, entry.Value, entry.TTL)
			if obj.IsExpired(now) {
				if _, loaded := s.data.LoadAndDelete(entry.Key); loaded {
					atomic.AddInt64(&s.count, -1)
				}
				return nil
			}
			if _, loaded := s.data.LoadOrStore(entry.Key, obj); loaded {
				s.data.Store(entry.Key, obj)
			} else {
				atomic.AddInt64(&s.count, 1)
			}
		case wal.DeleteOperation:
			if _, loaded := s.data.LoadAndDelete(entry.Key); loaded {
				atomic.AddInt64(&s.count, -1)
			}
		}
		return nil
	})
}
