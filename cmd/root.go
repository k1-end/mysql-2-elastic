package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
  Use:   "my2el",
  Short: "MySQL to ElasticSearch",
  Long:  "MySQL to ElasticSearch",
  Run: func(cmd *cobra.Command, args []string) {
  },
}

func Execute() {
  if err := rootCmd.Execute(); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
}
