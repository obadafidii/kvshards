package store

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store/objects"
	"sync"
	"sync/atomic"
)

// Storage is an interface that defines the operations that can be performed on the store.
type Storage interface {
	Get(key string) (*objects.Object, bool, error)
	Put(key string, value *objects.Object) error
	Size() int64
	Delete(key string) error
	Exists(key string) bool
}

type Store struct {
	shardID int
	Data    *sync.Map
	count   int64
}

func NewStore(shardID int) (s *Store) {
	s = &Store{
		shardID: shardID,
		Data:    &sync.Map{},
		count:   0,
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
	s.Data.Store(key, value)
	atomic.AddInt64(&s.count, 1)
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
