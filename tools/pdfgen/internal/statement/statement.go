// Package statement builds deterministic sample bank statements.
package statement

import (
	"fmt"
	"math/rand"
	"time"
)

// Transaction is one rendered table row. Amounts are integer cents so
// formatting can never introduce a rounding surprise.
type Transaction struct {
	Date         time.Time
	Description  string
	AmountCents  int64
	BalanceCents int64
}

// Statement is everything needed to render one PDF file.
type Statement struct {
	AccountNumber string
	PeriodStart   time.Time
	PeriodEnd     time.Time
	RowsPerPage   int
	Pages         int
	Transactions  []Transaction
}

// DateLayout is the date format used in the PDF.
const DateLayout = "2006-01-02"

// merchants are ASCII-only and comma-free: fpdf core fonts are latin-1, and a
// comma would force quoting in the CSV the worker emits.
var merchants = []string{
	"AMAZON MARKETPLACE",
	"SHELL GAS STATION",
	"WHOLE FOODS MARKET",
	"STARBUCKS STORE 4412",
	"UBER TRIP HELP.UBER.COM",
	"NETFLIX SUBSCRIPTION",
	"SPOTIFY PREMIUM",
	"WALMART SUPERCENTER",
	"TARGET STORE T-1902",
	"COSTCO WHOLESALE",
	"APPLE STORE ONLINE",
	"GOOGLE CLOUD PLATFORM",
	"DELTA AIR LINES",
	"MARRIOTT HOTELS",
	"CVS PHARMACY 8871",
	"HOME DEPOT 6602",
	"BEST BUY 0231",
	"MCDONALDS F1204",
	"CHIPOTLE ONLINE",
	"TRADER JOES 553",
	"VERIZON WIRELESS PMT",
	"COMCAST XFINITY BILL",
	"STATE FARM INSURANCE",
	"CITY WATER UTILITY",
	"PLANET FITNESS DUES",
	"LYFT RIDE",
}

var credits = []string{
	"ACME PAYROLL DIRECT DEP",
	"CLIENT INVOICE PAYMENT",
	"IRS TREAS 310 TAX REF",
	"INTEREST PAYMENT",
	"ZELLE TRANSFER IN",
}

// Options controls generation of a single statement.
type Options struct {
	Index       int // 0-based file index, folded into the account number
	Pages       int
	RowsPerPage int
	Rand        *rand.Rand
}

// Generate builds one statement. Given the same seeded *rand.Rand and the same
// call order, the result is byte-for-byte reproducible.
func Generate(opt Options) Statement {
	r := opt.Rand
	total := opt.Pages * opt.RowsPerPage

	st := Statement{
		AccountNumber: fmt.Sprintf("%04d-%04d-%04d", 1000+opt.Index, r.Intn(9000)+1000, r.Intn(9000)+1000),
		RowsPerPage:   opt.RowsPerPage,
		Pages:         opt.Pages,
		Transactions:  make([]Transaction, 0, total),
	}

	day := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, opt.Index, 0)
	st.PeriodStart = day

	balance := int64(r.Intn(900000) + 100000) // 1000.00 .. 10000.00
	for i := 0; i < total; i++ {
		if r.Intn(3) == 0 {
			day = day.AddDate(0, 0, 1)
		}

		var desc string
		var amount int64
		if r.Intn(5) == 0 {
			desc = credits[r.Intn(len(credits))]
			amount = int64(r.Intn(400000) + 5000)
		} else {
			desc = merchants[r.Intn(len(merchants))]
			amount = -int64(r.Intn(30000) + 100)
		}
		balance += amount

		st.Transactions = append(st.Transactions, Transaction{
			Date:         day,
			Description:  desc,
			AmountCents:  amount,
			BalanceCents: balance,
		})
	}
	st.PeriodEnd = day

	return st
}

// FormatCents renders cents with a sign and exactly two decimals, e.g. -123.45.
func FormatCents(c int64) string {
	sign := ""
	if c < 0 {
		sign = "-"
		c = -c
	}
	return fmt.Sprintf("%s%d.%02d", sign, c/100, c%100)
}

// Amount is the amount column exactly as it appears in the PDF.
func (t Transaction) Amount() string { return FormatCents(t.AmountCents) }

// Balance is the balance column exactly as it appears in the PDF.
func (t Transaction) Balance() string { return FormatCents(t.BalanceCents) }

// DateString is the date column exactly as it appears in the PDF.
func (t Transaction) DateString() string { return t.Date.Format(DateLayout) }
