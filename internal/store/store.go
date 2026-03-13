package store


type Store struct {
	Data map[string]string
}

func NewStore() (s *Store) {
	s = &Store{
		Data: make(map[string]string),
	}
	return s
}