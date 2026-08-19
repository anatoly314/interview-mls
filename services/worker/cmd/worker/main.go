package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/anatolyt/interview-mls/services/worker/internal/app"
	"github.com/anatolyt/interview-mls/services/worker/internal/config"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	root := &cobra.Command{
		Use:   "worker",
		Short: "MLS worker: claims jobs from postgres, parses pdf to csv",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(config.FromEnv(), log).Run(context.Background())
		},
	}

	if err := root.Execute(); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}
