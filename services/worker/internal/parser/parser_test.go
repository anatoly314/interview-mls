package parser

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The fixtures were generated with `pdfgen --pages 3 --rows 30 --seed 1337`.
const fixtureRows = 3 * 30

// Pinned rows of statement-001.pdf: the seed makes the generator reproducible,
// so these are stable and catch a silent shift in cell alignment that a
// format-only check would pass.
var (
	firstOf001 = Transaction{Date: "2024-01-01", Description: "APPLE STORE ONLINE", Amount: "-289.94", Balance: "1112.05"}
	lastOf001  = Transaction{Date: "2024-02-01", Description: "WALMART SUPERCENTER", Amount: "-41.10", Balance: "27973.86"}
)

func TestParseFixtures(t *testing.T) {
	pdfs, err := filepath.Glob("testdata/*.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if len(pdfs) == 0 {
		t.Fatal("no testdata pdfs")
	}

	for _, pdfPath := range pdfs {
		t.Run(filepath.Base(pdfPath), func(t *testing.T) {
			raw, err := os.ReadFile(pdfPath)
			if err != nil {
				t.Fatal(err)
			}
			txs, err := Parse(raw)
			if err != nil {
				t.Fatal(err)
			}

			if len(txs) != fixtureRows {
				t.Fatalf("parsed %d transactions, want %d", len(txs), fixtureRows)
			}

			prevDate := ""
			for i, tx := range txs {
				if !dateRe.MatchString(tx.Date) {
					t.Errorf("row %d: date %q malformed", i, tx.Date)
				}
				if tx.Description == "" {
					t.Errorf("row %d: empty description", i)
				}
				if !amtRe.MatchString(tx.Amount) {
					t.Errorf("row %d: amount %q malformed", i, tx.Amount)
				}
				if !amtRe.MatchString(tx.Balance) {
					t.Errorf("row %d: balance %q malformed", i, tx.Balance)
				}
				// ISO dates sort lexically, and the generator only ever moves
				// the day forward, so statement order must be non-decreasing.
				if tx.Date < prevDate {
					t.Errorf("row %d: date %q precedes previous %q", i, tx.Date, prevDate)
				}
				prevDate = tx.Date
			}

			if filepath.Base(pdfPath) == "statement-001.pdf" {
				if txs[0] != firstOf001 {
					t.Errorf("first transaction = %+v, want %+v", txs[0], firstOf001)
				}
				if got := txs[len(txs)-1]; got != lastOf001 {
					t.Errorf("last transaction = %+v, want %+v", got, lastOf001)
				}
			}
		})
	}
}

func TestToCSV(t *testing.T) {
	got, err := ToCSV([]Transaction{firstOf001, lastOf001})
	if err != nil {
		t.Fatal(err)
	}
	want := "date,description,amount,balance\n" +
		"2024-01-01,APPLE STORE ONLINE,-289.94,1112.05\n" +
		"2024-02-01,WALMART SUPERCENTER,-41.10,27973.86\n"
	if string(got) != want {
		t.Errorf("ToCSV =\n%s\nwant\n%s", got, want)
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	if _, err := Parse([]byte("%PDF-1.4 not really a pdf")); err == nil {
		t.Error("expected error for malformed pdf")
	}
}

// TestParseMalformedNeverPanics: bad uploads must come back as errors, never
// as a panic that would kill the consumer loop.
func TestParseMalformedNeverPanics(t *testing.T) {
	valid, err := os.ReadFile("testdata/statement-001.pdf")
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"empty":            {},
		"header only":      []byte("%PDF-1.4\n"),
		"header + garbage": append([]byte("%PDF-1.4\n"), bytes.Repeat([]byte{0xff, 0x00, 0x7f}, 400)...),
		"text file":        []byte("hello, this is not a pdf at all"),
		"truncated head":   valid[:len(valid)/2],
		"truncated tail":   valid[len(valid)/2:],
		"missing trailer":  valid[:len(valid)-64],
		"nul padded":       append(append([]byte{}, valid...), make([]byte, 512)...),
	}
	// flipping bytes across the xref/trailer region is what tends to trip the
	// reader's unchecked indexing
	for i := 0; i < 8; i++ {
		b := append([]byte{}, valid...)
		b[len(b)-1-i*7] ^= 0xff
		cases[fmt.Sprintf("corrupt trailer byte -%d", 1+i*7)] = b
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			txs, err := Parse(raw)
			if err == nil && len(txs) != fixtureRows {
				t.Errorf("no error but %d transactions parsed", len(txs))
			}
			// the point of the case is that we got here at all
		})
	}
}
