package store

import "kvshard/internal/commands"

type Store struct {
	Data map[string]string	
}

func NewStore() (s *Store) {
	s = &Store{
		Data: make(map[string]string),
	}
	return s
}


func (s *Store) Execute(cmd *commands.Command) (*commands.Result, error) {
	return nil, nil
}