package commands

import "kvshard/internal/store"

type Command struct {
	Name string
	Args []string
	// this helps to validate the input
	MaxArgs, MinArgs int
	Description      string
	Execute          func(store store.Storage, args []string) (*Result, error)
}

var Registry map[string]*Command

var putCmd = &Command{
	Name:        "put",
	MaxArgs:     3,
	MinArgs:     2,
	Description: "PUT KEY VALUE [TTL] -- TTL is in microseconds",
	Execute:     put,
}

var getCmd = &Command{
	Name:        "get",
	MaxArgs:     1,
	MinArgs:     1,
	Description: "GET KEY",
	Execute:     get,
}

var delCmd = &Command{
	Name:        "del",
	MaxArgs:     1,
	MinArgs:     1,
	Description: "DEL KEY",
	Execute:     del,
}

var existsCmd = &Command{
	Name:        "exists",
	MaxArgs:     1,
	MinArgs:     1,
	Description: "EXISTS KEY",
	Execute:     exists,
}

func init() {
	Registry = make(map[string]*Command)
	Registry["put"] = putCmd
	Registry["get"] = getCmd
	Registry["del"] = delCmd
	//Registry["exists"] = existsCmd //TODO: to be done later
}
