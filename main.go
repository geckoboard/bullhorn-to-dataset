package main

import (
	"bullhorn-to-dataset/cmd"
	"os"
)

func main() {
	root := cmd.Setup()

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
