package cmd

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var (
	logger     *slog.Logger
	noOfShards int
	verbose    bool
)

func init() {
	logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

var rootCmd = &cobra.Command{
	Use:   "kvshard",
	Short: "A key-value store that is natively sharded",
	Long: `A key-value store that is natively sharded.

This is a key-value store that is natively sharded, shards can be configurable.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info("kvshard", "version", "0.0.1", "shards", noOfShards, "verbose", verbose)
	},
}

func Execute() {
	rootCmd.PersistentFlags().IntVarP(&noOfShards, "shards", "s", 1, "Number of shards")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")

	if err := rootCmd.Execute(); err != nil {
		logger.Error("failed to execute root command", "error", err)
		os.Exit(1)
	}
}
