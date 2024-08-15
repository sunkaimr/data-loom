package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	gitCommit string
	buildTime string
	goVersion string
	version   string
)

func NewVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "version",
		Short:   "version",
		Example: "data-loom version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("{\"version\":\"%s\",\"gitCommit\":\"%s\",\"buildTime\":\"%s\",\"goVersion\":\"%s\"}", version, gitCommit, buildTime, goVersion)
		},
	}
}
