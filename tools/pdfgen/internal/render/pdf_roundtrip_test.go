package render_test

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ledongthuc/pdf"

	"github.com/anatolyt/interview-mls/tools/pdfgen/internal/render"
	"github.com/anatolyt/interview-mls/tools/pdfgen/internal/statement"
)

func generate(t *testing.T, pages, rows int) (statement.Statement, string) {
	t.Helper()
	st := statement.Generate(statement.Options{
		Pages:       pages,
		RowsPerPage: rows,
		Rand:        rand.New(rand.NewSource(42)),
	})
	path := filepath.Join(t.TempDir(), "statement-001.pdf")
	if err := render.Render(st, path); err != nil {
		t.Fatalf("render: %v", err)
	}
	return st, path
}

// cells splits extracted page text into the drawn cells. ledongthuc/pdf emits a
// newline per PDF text object (BT), and every CellFormat call is its own text
// object, so one non-empty line == one table cell, in draw order.
func cells(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

// TestRoundTripPerPage is the parsing contract: each page extracts as
// title, account line, period line, 4 column headers, 4 cells per transaction,
// then the page footer -- every field byte-identical to what was generated.
func TestRoundTripPerPage(t *testing.T) {
	const pages, rows = 3, 25
	st, path := generate(t, pages, rows)

	f, r, err := pdf.Open(path)
	if err != nil {
		t.Fatalf("open pdf: %v", err)
	}
	defer f.Close()

	if got := r.NumPage(); got != pages {
		t.Fatalf("NumPage = %d, want %d", got, pages)
	}

	for p := 1; p <= pages; p++ {
		text, err := r.Page(p).GetPlainText(nil)
		if err != nil {
			t.Fatalf("page %d GetPlainText: %v", p, err)
		}
		got := cells(text)

		want := []string{
			render.Title,
			"Account Number: " + st.AccountNumber,
			"Statement Period: " + st.PeriodStart.Format(statement.DateLayout) +
				" to " + st.PeriodEnd.Format(statement.DateLayout),
			"Date", "Description", "Amount", "Balance",
		}
		for _, tx := range st.Transactions[(p-1)*rows : p*rows] {
			want = append(want, tx.DateString(), tx.Description, tx.Amount(), tx.Balance())
		}
		want = append(want, "Page "+strconv.Itoa(p))

		if len(got) != len(want) {
			t.Fatalf("page %d: extracted %d cells, want %d\ngot: %q", p, len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("page %d cell %d = %q, want %q", p, i, got[i], want[i])
			}
		}
	}
}

// TestRoundTripWholeDocument asserts every field of every transaction also
// appears in order in the flat whole-document text stream.
func TestRoundTripWholeDocument(t *testing.T) {
	st, path := generate(t, 2, 25)

	f, r, err := pdf.Open(path)
	if err != nil {
		t.Fatalf("open pdf: %v", err)
	}
	defer f.Close()

	buf, err := r.GetPlainText()
	if err != nil {
		t.Fatalf("GetPlainText: %v", err)
	}
	b, err := io.ReadAll(buf)
	if err != nil {
		t.Fatalf("read text: %v", err)
	}
	text := string(b)

	if !strings.Contains(text, render.Title) {
		t.Fatalf("extracted text missing title %q", render.Title)
	}

	pos := 0
	for i, tx := range st.Transactions {
		for _, field := range []string{tx.DateString(), tx.Description, tx.Amount(), tx.Balance()} {
			idx := strings.Index(text[pos:], field)
			if idx < 0 {
				t.Fatalf("transaction %d: field %q not found in order after offset %d", i, field, pos)
			}
			pos += idx + len(field)
		}
	}
}

// TestDeterministic guards the --seed contract.
func TestDeterministic(t *testing.T) {
	dir := t.TempDir()
	var prev string
	for i := 0; i < 2; i++ {
		st := statement.Generate(statement.Options{Pages: 1, RowsPerPage: 5, Rand: rand.New(rand.NewSource(7))})
		p := filepath.Join(dir, "s.pdf")
		if err := render.Render(st, p); err != nil {
			t.Fatalf("render: %v", err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && prev != string(b) {
			t.Fatal("same seed produced different PDF bytes")
		}
		prev = string(b)
	}
}
