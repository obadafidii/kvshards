package store

import (
	"kvshard/internal/kverrors"
	"sync"
	"time"
)

// Storage is an interface that defines the operations that can be performed on the store.
type Storage interface {
	Get(key string) (string, bool, error)
	Put(key, value string) error
	Delete(key string) error
	Exists(key string) bool
}

// Data
// TODO: improve the Data in Store, so that we can be tracking time to live.
type Data struct {
	val any
	ttl time.Duration
}

type Store struct {
	shardID int
	Data    *sync.Map
}

func NewStore(shardID int) (s *Store) {
	s = &Store{
		shardID: shardID,
		Data:    &sync.Map{},
	}
	return s
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
	return err
}

func (s *Store) Put(key, value string) (err error) {
	s.Data.Store(key, value)
	return err
}

func (s *Store) Get(key string) (value string, ok bool, err error) {
	val, ok := s.Data.Load(key)
	if !ok {
		err = kverrors.ErrKeyNotFound
		return value, ok, err
	}
	value = val.(string)
	return value, ok, err
}
