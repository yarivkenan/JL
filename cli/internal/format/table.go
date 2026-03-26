// Package format provides human-readable output formatters for CLI commands.
package format

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// Table writes aligned columnar output.
type Table struct {
	w       *tabwriter.Writer
	headers []string
}

// NewTable creates a table that writes to w with the given column headers.
func NewTable(w io.Writer, headers ...string) *Table {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	t := &Table{w: tw, headers: headers}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	fmt.Fprintln(tw, strings.Repeat("─\t", len(headers)))
	return t
}

// Row writes a single row. Values are tab-separated.
func (t *Table) Row(vals ...string) {
	fmt.Fprintln(t.w, strings.Join(vals, "\t"))
}

// Flush writes all buffered output.
func (t *Table) Flush() { t.w.Flush() }

// KV prints a key-value detail block, one pair per line.
func KV(w io.Writer, pairs ...string) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for i := 0; i+1 < len(pairs); i += 2 {
		fmt.Fprintf(tw, "%s:\t%s\n", pairs[i], pairs[i+1])
	}
	tw.Flush()
}
