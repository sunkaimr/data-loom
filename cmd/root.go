package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "data-loom",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%s\n", "Use -h to see more commands")
	},
}

func Execute() {
	rootCmd.AddCommand(
		NewVersionCmd(),
		NewServerCmd(),
		NewTokenCmd(),
	)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
