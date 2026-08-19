package statement

import (
	"encoding/csv"
	"fmt"
	"os"
)

// CSVHeader is the ground-truth CSV header the parser is expected to produce.
var CSVHeader = []string{"date", "description", "amount", "balance"}

// WriteCSV writes the ground-truth CSV for st: exactly the rows rendered into
// the PDF, in the same order, with identically formatted amounts.
func WriteCSV(st Statement, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write(CSVHeader); err != nil {
		return err
	}
	for _, t := range st.Transactions {
		if err := w.Write([]string{t.DateString(), t.Description, t.Amount(), t.Balance()}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}
