package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/anatolyt/interview-mls/services/api/internal/app"
	"github.com/anatolyt/interview-mls/services/api/internal/config"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	root := &cobra.Command{
		Use:   "api",
		Short: "MLS api: pdf upload, job status, csv download, websocket notifications",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(config.FromEnv(), log).Run(context.Background())
		},
	}

	if err := root.Execute(); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}
