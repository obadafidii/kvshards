package commands

var Registry map[string]*Command

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
	Registry = make(map[string]*Command)
	Registry["put"] = putCmd
	Registry["get"] = getCmd
	Registry["del"] = delCmd
	//Registry["exists"] = existsCmd //TODO: to be done later
}
