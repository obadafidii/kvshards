package commands

var registry map[string]*Command

var putCmd = &Command{
	Name:        "put",
	Description: "PUT KEY VALUE",
	Execute:     put,
}

var getCmd = &Command{
	Name:        "get",
	Description: "GET KEY",
	Execute:     get,
}

var delCmd = &Command{
	Name:        "del",
	Description: "DEL KEY",
	Execute:     del,
}

var existsCmd = &Command{
	Name:        "exists",
	Description: "EXISTS KEY",
	Execute:     exists,
}

func init() {
	registry = make(map[string]*Command)
	registry["put"] = putCmd
	registry["get"] = getCmd
	registry["del"] = delCmd
	registry["exists"] = existsCmd
}
