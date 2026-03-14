package cmd

import (
	"context"
	"kvshard/internal/server"
	"log/slog"
	"os"
	"runtime"

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
		// cpu tunning
		if noOfShards == 0 {
			// if user does not specify shard count, we default to all possible cores
			noOfShards = runtime.NumCPU()
		}

		runtime.GOMAXPROCS(noOfShards)
		logger.Info("kvshard", "version", "0.0.1", "shards", noOfShards, "verbose", verbose)

		server.Start(context.Background(), logger, noOfShards)
	},
}

func Execute() {
	rootCmd.PersistentFlags().IntVarP(&noOfShards, "shards", "s", 0, "Number of shards")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")

	if err := rootCmd.Execute(); err != nil {
		logger.Error("failed to execute root command", "error", err)
		os.Exit(1)
	}
}
