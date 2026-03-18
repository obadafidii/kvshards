package store

import (
	"fmt"
	"kvshard/internal/kverrors"
	"kvshard/internal/store/objects"
)

// Storage is an interface that defines the operations that can be performed on the store.
type Storage interface {
	Get(key string) (*objects.Object, bool, error)
	Put(key string, value *objects.Object) error
	Size() int
	Delete(key string) error
	Exists(key string) bool
}

type Store struct {
	shardID int
	Data    map[string]*objects.Object
}

func NewStore(shardID int) (s *Store) {
	s = &Store{
		shardID: shardID,
		Data:    make(map[string]*objects.Object),
	}
	return s
}

func (s *Store) Size() int {
	return len(s.Data)
}

func (s *Store) Exists(key string) bool {
	if _, ok := s.Data[key]; ok {
		return true
	}
	return false
}

func (s *Store) Delete(key string) (err error) {
	_, ok := s.Data[key]
	if !ok {
		return kverrors.ErrKeyNotFound
	}
	delete(s.Data, key)
	return err
}

func (s *Store) Put(key string, value *objects.Object) (err error) {
	s.Data[key] = value
	return err
}

func (s *Store) Get(key string) (value *objects.Object, ok bool, err error) {
	value, ok = s.Data[key]
	if !ok {
		err = kverrors.ErrKeyNotFound
		return value, ok, err
	}
	return value, ok, err
}

func (s *Store) ExpiredObjects() (expired []*objects.Object) {
	for id, object := range s.Data {
		fmt.Printf("id=%s, object=%v", id, object)

		if isExpired(object) {
			expired = append(expired, object)
		}
	}

	return expired
}

func isExpired(obj *objects.Object) bool {
	return false
}
