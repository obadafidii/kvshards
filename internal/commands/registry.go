package commands

var registry map[string]*Command

var helpCmd = &Command{
	Name: "help",
	Description: "",
	Run: func(args []string) (Result, error) {
		return Result{}, nil
	},
}

var putCmd = &Command{
	Name: "put",
	Description: "PUT KEY VALUE",
	Run: func(args []string) (Result, error) {
		return Result{}, nil
	},
}

var getCmd = &Command{
	Name: "get",
	Description: "GET KEY",
	Run: func(args []string) (Result, error) {
		return Result{}, nil
	},
}

var delCmd = &Command{
	Name: "del",
	Description: "DEL KEY",
	Run: func(args []string) (Result, error) {
		return Result{}, nil
	},
}

var existsCmd = &Command{
	Name: "exists",
	Description: "EXISTS KEY",
	Run: func(args []string) (Result, error) {
		return Result{}, nil
	},
}

func init() {
	registry = make(map[string]*Command)
	registry["help"] = helpCmd
	registry["put"] = putCmd
	registry["get"] = getCmd
	registry["del"] = delCmd
	registry["exists"] = existsCmd
}
