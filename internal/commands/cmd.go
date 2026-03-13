package commands

type Result struct {

}

type Command struct {
	Name        string
	Args        []string
	Description string
	Run         func(args []string) (Result, error)
}

