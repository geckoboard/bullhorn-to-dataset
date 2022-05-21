package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var version = ""

func Setup() *cobra.Command {
	root := &cobra.Command{
		Use:   "bullhorn-to-dataset",
		Short: `Push your Bullhorn data such as job posts to your Geckoboard dataset`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Hidden: true,
	}

	root.Run = func(cmd *cobra.Command, args []string) {
		curr, _, _ := root.Find(os.Args[1:])

		// Default to help if no commands present
		if curr.Use == root.Use {
			root.SetArgs([]string{"-h"})
			root.Execute()
		}
	}

	root.AddCommand(VersionCommand())
	root.AddCommand(PushCommand())

	return root
}
