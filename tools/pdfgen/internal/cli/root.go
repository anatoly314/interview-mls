// Package cli wires the pdfgen command line.
package cli

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/anatolyt/interview-mls/tools/pdfgen/internal/render"
	"github.com/anatolyt/interview-mls/tools/pdfgen/internal/statement"
)

type options struct {
	out   string
	count int
	pages int
	rows  int
	seed  int64
}

// NewRootCmd builds the pdfgen root command.
func NewRootCmd() *cobra.Command {
	var opt options

	cmd := &cobra.Command{
		Use:   "pdfgen",
		Short: "Generate sample bank-statement PDFs with ground-truth CSVs",
		Long: "pdfgen writes statement-NNN.pdf sample bank statements plus a matching\n" +
			"statement-NNN.expected.csv holding exactly the rows rendered into each PDF.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opt)
		},
	}

	f := cmd.Flags()
	f.StringVar(&opt.out, "out", "./samples", "output directory")
	f.IntVar(&opt.count, "count", 3, "number of statement files to generate")
	f.IntVar(&opt.pages, "pages", 2, "pages per file")
	f.IntVar(&opt.rows, "rows", 25, "transaction rows per page")
	f.Int64Var(&opt.seed, "seed", 42, "random seed; identical seeds produce identical output")

	return cmd
}

func run(cmd *cobra.Command, opt options) error {
	if opt.count < 1 || opt.pages < 1 || opt.rows < 1 {
		return fmt.Errorf("--count, --pages and --rows must all be >= 1")
	}
	if opt.rows > render.MaxRowsPerPage {
		return fmt.Errorf("--rows must be <= %d (rows beyond that don't fit an A4 page)", render.MaxRowsPerPage)
	}
	if err := os.MkdirAll(opt.out, 0o755); err != nil {
		return err
	}

	r := rand.New(rand.NewSource(opt.seed))
	for i := 0; i < opt.count; i++ {
		st := statement.Generate(statement.Options{
			Index:       i,
			Pages:       opt.pages,
			RowsPerPage: opt.rows,
			Rand:        r,
		})

		base := filepath.Join(opt.out, fmt.Sprintf("statement-%03d", i+1))
		pdfPath := base + ".pdf"
		csvPath := base + ".expected.csv"

		if err := render.Render(st, pdfPath); err != nil {
			return fmt.Errorf("render %s: %w", pdfPath, err)
		}
		if err := statement.WriteCSV(st, csvPath); err != nil {
			return fmt.Errorf("write %s: %w", csvPath, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s (%d rows)\n%s\n", pdfPath, len(st.Transactions), csvPath)
	}
	return nil
}
