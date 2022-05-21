package cmd

import (
	"fmt"
	"log"
	"runtime/debug"

	"github.com/spf13/cobra"
)

func VersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version info",
		Run: func(cmd *cobra.Command, args []string) {
			build, ok := debug.ReadBuildInfo()
			if !ok {
				log.Fatal("failed to get build info")
			}

			fmt.Println("Built with:", build.GoVersion)
			fmt.Println("GitSHA:", extractVCSVersion(build.Settings)[:10])
			fmt.Println("Version:", version)
		},
	}
}

func extractVCSVersion(settings []debug.BuildSetting) string {
	for _, v := range settings {
		if v.Key == "vcs.revision" {
			return v.Value
		}
	}

	return "(not set)"
}
