// Package parser converts ACME Bank statement PDFs (produced by tools/pdfgen)
// into CSV. The PDF layout is a contract: extraction with GetPlainText yields
// one table cell per line in draw order, so each page reads as 7 preamble
// cells (title, account, period, 4 column headers), then 4 cells per
// transaction row, then a "Page N" footer. The row-based extraction API is
// useless here — fpdf positions text with Td, which ledongthuc/pdf ignores.
package parser

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"regexp"
	"strings"

	"github.com/ledongthuc/pdf"
)

type Transaction struct {
	Date        string
	Description string
	Amount      string
	Balance     string
}

var (
	dateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	amtRe  = regexp.MustCompile(`^-?\d+\.\d{2}$`)
	pageRe = regexp.MustCompile(`^Page \d+$`)
)

func isPreamble(line string) bool {
	switch line {
	case "ACME Bank - Account Statement", "Date", "Description", "Amount", "Balance":
		return true
	}
	return strings.HasPrefix(line, "Account Number:") ||
		strings.HasPrefix(line, "Statement Period:") ||
		pageRe.MatchString(line)
}

// Parse extracts all transactions from a statement PDF.
//
// ledongthuc/pdf indexes into object streams with little validation and
// panics on some malformed inputs; a panic here would take down the worker
// for what is really just a bad upload, so it is converted to an error and
// travels the normal retry-then-fail path.
func Parse(raw []byte) (txs []Transaction, err error) {
	defer func() {
		if r := recover(); r != nil {
			txs, err = nil, fmt.Errorf("pdf parse panic: %v", r)
		}
	}()

	doc, err := pdf.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return nil, fmt.Errorf("read pdf: %w", err)
	}
	textReader, err := doc.GetPlainText()
	if err != nil {
		return nil, fmt.Errorf("extract text: %w", err)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(textReader); err != nil {
		return nil, fmt.Errorf("extract text: %w", err)
	}

	var cells []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || isPreamble(line) {
			continue
		}
		cells = append(cells, line)
	}

	if len(cells) == 0 {
		return nil, fmt.Errorf("no transaction rows found: not an ACME statement?")
	}
	if len(cells)%4 != 0 {
		return nil, fmt.Errorf("unrecognized statement layout: %d cells not divisible by 4", len(cells))
	}

	txs = make([]Transaction, 0, len(cells)/4)
	for i := 0; i < len(cells); i += 4 {
		t := Transaction{
			Date:        cells[i],
			Description: cells[i+1],
			Amount:      cells[i+2],
			Balance:     cells[i+3],
		}
		if !dateRe.MatchString(t.Date) {
			return nil, fmt.Errorf("row %d: %q is not a date", i/4+1, t.Date)
		}
		if !amtRe.MatchString(t.Amount) {
			return nil, fmt.Errorf("row %d: %q is not an amount", i/4+1, t.Amount)
		}
		if !amtRe.MatchString(t.Balance) {
			return nil, fmt.Errorf("row %d: %q is not a balance", i/4+1, t.Balance)
		}
		txs = append(txs, t)
	}
	return txs, nil
}

// ToCSV renders transactions as header + one row per transaction, using
// encoding/csv defaults.
func ToCSV(txs []Transaction) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{"date", "description", "amount", "balance"}); err != nil {
		return nil, err
	}
	for _, t := range txs {
		if err := w.Write([]string{t.Date, t.Description, t.Amount, t.Balance}); err != nil {
			return nil, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
