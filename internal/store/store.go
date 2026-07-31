package store

import (
	"context"
	"kvshard/internal/kverrors"
	"kvshard/internal/store/objects"
	"kvshard/internal/store/wal"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Storage is an interface that defines the operations that can be performed on the store.
type Storage interface {
	Get(key string) (*objects.Object, bool, error)
	Put(key string, value *objects.Object) error
	Size() int64
	Flush() error
	Delete(key string) error
	Exists(key string) bool
}

type Store struct {
	shardID int
	Data    *sync.Map
	count   int64
	wal     *wal.WAL
}

func NewStore(ctx context.Context, shardID int, logger *slog.Logger) (s *Store) {
	s = &Store{
		shardID: shardID,
		Data:    &sync.Map{},
		count:   0,
		wal:     wal.New(ctx, shardID, logger),
	}

	return s
}

func (s *Store) Size() int64 {
	return s.count
}

func (s *Store) Exists(key string) bool {
	if _, ok := s.Data.Load(key); ok {
		return true
	}
	return false
}

func (s *Store) Delete(key string) (err error) {
	_, ok := s.Data.Load(key)
	if !ok {
		return kverrors.ErrKeyNotFound
	}
	s.Data.Delete(key)
	atomic.AddInt64(&s.count, -1)
	return err
}

func (s *Store) Put(key string, value *objects.Object) (err error) {
	// write to WAL before applying the change to the in-memory store to ensure durability and consistency.

	//TODO:
	// if the WAL write fails, what should I do? we can either return an error to the caller or we can retry the WAL write a certain number of times before giving up.
	entry := &wal.WALEntry{
		Command: "put",
		Key:     key,
		Value:   value.String(),
		TTL:     value.TTL(),
	}
	if err := s.wal.Append(entry); err != nil {
		return err
	}

	s.Data.Store(key, value)

	atomic.AddInt64(&s.count, 1)
	return err
}

func (s *Store) Flush() (err error) {
	// flush the WAL buffer to the disk to ensure that all data is persisted before shutting down the shard.
	if err := s.wal.Close(); err != nil {
		return err
	}

	return err
}

func (s *Store) Get(key string) (value *objects.Object, ok bool, err error) {
	val, ok := s.Data.Load(key)
	if !ok {
		err = kverrors.ErrKeyNotFound
		return value, ok, err
	}
	value = val.(*objects.Object)
	return value, ok, err
}
