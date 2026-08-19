// Command pdfgen generates sample bank-statement PDFs and their ground-truth CSVs.
package main

import (
	"os"

	"github.com/anatolyt/interview-mls/tools/pdfgen/internal/cli"
)

func main() {
	if err := cli.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
