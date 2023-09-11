package main

import (
	"sample-producer/cmd"
	"github.com/pkg/errors"
	"os"
)

func main() {
	if err := run(os.Args); err != nil {
		panic(errors.Wrap(err, "fatal error"))
	}
}

func run(args []string) error {

	producerCommand, err := cmd.CreateProducerCmd()
	if err != nil {
		return errors.Wrap(err, "Failed to create producer command")
	}

	return producerCommand.Execute()
}
