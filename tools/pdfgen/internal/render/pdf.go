// Package render draws statements as PDFs whose text layer extracts cleanly.
package render

import (
	"strconv"
	"time"

	"github.com/go-pdf/fpdf"

	"github.com/anatolyt/interview-mls/tools/pdfgen/internal/statement"
)

// Title is the first line of every page.
const Title = "ACME Bank - Account Statement"

// Column geometry in mm, page A4 portrait with 15mm margins.
//
// Every cell is drawn left-aligned with CellFormat, so each column's text
// starts at exactly these X offsets on every page. Text extractors report the
// cell's start X, so left alignment (rather than "R" on the numeric columns)
// is what makes the column positions a stable parsing contract.
const (
	colDateX = 15.0
	colDescX = 45.0
	colAmtX  = 130.0
	colBalX  = 162.0

	colDateW = 30.0
	colDescW = 85.0
	colAmtW  = 32.0
	colBalW  = 33.0

	rowH       = 6.0
	tableTopY  = 45.0

	// MaxRowsPerPage is how many rows fit between tableTopY and the footer
	// band (~282mm on A4) at rowH; auto page break is off, so more rows
	// would draw over the footer instead of wrapping.
	MaxRowsPerPage = 39

	fontFamily = "Courier" // monospaced latin-1 core font: no embedded font, no glyph remapping
	fontSize   = 9.0
)

// Render writes st to path as a PDF.
func Render(st statement.Statement, path string) error {
	pdf := fpdf.New("P", "mm", "A4", "")
	pdf.SetMargins(15, 15, 15)
	pdf.SetAutoPageBreak(false, 15) // rows per page is fixed by the caller, not by overflow
	// Fixed timestamp keeps output byte-identical for a given seed.
	pdf.SetCreationDate(time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC))

	pdf.SetHeaderFunc(func() { drawHeader(pdf, st) })
	pdf.SetFooterFunc(func() { drawFooter(pdf) })

	for i, t := range st.Transactions {
		if i%st.RowsPerPage == 0 {
			pdf.AddPage()
			pdf.SetXY(colDateX, tableTopY)
		}
		pdf.SetFont(fontFamily, "", fontSize)
		y := pdf.GetY()
		cell(pdf, colDateX, y, colDateW, t.DateString())
		cell(pdf, colDescX, y, colDescW, t.Description)
		cell(pdf, colAmtX, y, colAmtW, t.Amount())
		cell(pdf, colBalX, y, colBalW, t.Balance())
		pdf.SetXY(colDateX, y+rowH)
	}

	return pdf.OutputFileAndClose(path)
}

func cell(pdf *fpdf.Fpdf, x, y, w float64, s string) {
	pdf.SetXY(x, y)
	pdf.CellFormat(w, rowH, s, "", 0, "L", false, 0, "")
}

// drawHeader repeats the title, the account meta lines and the column header on
// every page, so each page is self-describing for the parser.
func drawHeader(pdf *fpdf.Fpdf, st statement.Statement) {
	pdf.SetFont(fontFamily, "B", 12)
	cell(pdf, colDateX, 15, 120, Title)

	pdf.SetFont(fontFamily, "", fontSize)
	cell(pdf, colDateX, 24, 120, "Account Number: "+st.AccountNumber)
	cell(pdf, colDateX, 30, 120, "Statement Period: "+
		st.PeriodStart.Format(statement.DateLayout)+" to "+st.PeriodEnd.Format(statement.DateLayout))

	pdf.SetFont(fontFamily, "B", fontSize)
	cell(pdf, colDateX, 38, colDateW, "Date")
	cell(pdf, colDescX, 38, colDescW, "Description")
	cell(pdf, colAmtX, 38, colAmtW, "Amount")
	cell(pdf, colBalX, 38, colBalW, "Balance")
	pdf.Line(colDateX, 44, 195, 44)
}

func drawFooter(pdf *fpdf.Fpdf) {
	pdf.SetFont(fontFamily, "", 8)
	pdf.SetXY(colDateX, 282)
	pdf.CellFormat(180, 6, "Page "+strconv.Itoa(pdf.PageNo()), "", 0, "C", false, 0, "")
}
