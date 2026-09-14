package cmd

import (
	"context"
	"kvshard/internal/server"
	"kvshard/internal/store/wal"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

//TODO: I want to move the API/communication layer to gRPC instead of nc.

var (
	logger                 *slog.Logger
	noOfShards             int
	walEncoding            string
	walCompression         string
	walBatchSize           int
	walBatchInterval       time.Duration
	walAsyncCommit         bool
	walCompactionThreshold int64
)

func init() {
	logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

var rootCmd = &cobra.Command{
	Use:   "kvshard",
	Short: "A key-value store that is natively sharded",
	Long: `A key-value store that is natively sharded.

This is a key-value store that is natively sharded, shards can be configurable.`,
	RunE: func(cmd *cobra.Command, args []string) error {

		// cpu tunning
		if noOfShards == 0 {
			// if user does not specify shard count, we default to all possible cores
			noOfShards = runtime.NumCPU()
		}
		runtime.GOMAXPROCS(noOfShards)
		logger.Info("kvshard", "version", "0.0.1", "shards", noOfShards)

		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()

		walConfig := wal.Config{
			Encoding:            wal.Encoding(walEncoding),
			Compression:         wal.Compression(walCompression),
			BatchSize:           walBatchSize,
			BatchInterval:       walBatchInterval,
			AsyncCommit:         walAsyncCommit,
			CompactionThreshold: walCompactionThreshold,
		}
		return server.StartWithWALConfig(ctx, noOfShards, walConfig, logger)
	},
}

func Execute() {
	rootCmd.PersistentFlags().IntVarP(&noOfShards, "shards", "s", 0, "Number of shards")
	rootCmd.PersistentFlags().StringVar(&walEncoding, "wal-encoding", string(wal.JSONEncoding), "WAL encoding: json or binary")
	rootCmd.PersistentFlags().StringVar(&walCompression, "wal-compression", string(wal.NoCompression), "WAL compression: none or gzip")
	rootCmd.PersistentFlags().IntVar(&walBatchSize, "wal-batch-size", 1, "Records per WAL commit batch")
	rootCmd.PersistentFlags().DurationVar(&walBatchInterval, "wal-batch-interval", time.Second, "Maximum delay before committing a WAL batch")
	rootCmd.PersistentFlags().BoolVar(&walAsyncCommit, "wal-async-commit", false, "Return before WAL fsync; commit in the background")
	rootCmd.PersistentFlags().Int64Var(&walCompactionThreshold, "wal-compaction-threshold", 1000, "Minimum records before automatic WAL compaction (0 disables)")

	if err := rootCmd.Execute(); err != nil {
		logger.Error("failed to execute root command", "error", err)
		os.Exit(1)
	}
}
