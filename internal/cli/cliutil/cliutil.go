// Package cliutil holds tiny helpers shared across malcom subcommands.
//
// PeekFlag exists so subcommands can resolve `-config` and `-chain`
// from raw args BEFORE registering the rest of their flag set. That
// lets us register flags with defaults sourced from the loaded
// config — so `<subcommand> -h` shows the actual values the run will
// use (e.g. "(default 3000)") rather than placeholder zeroes.
//
// NiceUsage rewrites Go's default flag help output so config-sourced
// flags read as `(default X, sourced from Y)` instead of the
// awkward `(sourced from Y) (default X)`.
package cliutil

import (
	"bytes"
	"flag"
	"fmt"
	"regexp"
	"strings"
)

// PeekFlag scans argv for `-name VALUE`, `--name VALUE`, `-name=VALUE`,
// or `--name=VALUE` and returns VALUE. Returns "" if the flag wasn't
// passed.
//
// We don't validate the value or the rest of argv — this is a peek,
// not a parse. The real flag.FlagSet.Parse() call later catches any
// real syntax errors.
func PeekFlag(args []string, name string) string {
	short := "-" + name
	long := "--" + name
	for i, a := range args {
		switch a {
		case short, long:
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if v, ok := trim(a, short+"="); ok {
			return v
		}
		if v, ok := trim(a, long+"="); ok {
			return v
		}
	}
	return ""
}

func trim(s, prefix string) (string, bool) {
	if !strings.HasPrefix(s, prefix) {
		return "", false
	}
	return strings.TrimPrefix(s, prefix), true
}

// sourcedDefaultRe matches Go's flag.PrintDefaults output for a
// config-sourced flag: ` (sourced from KEY) (default VAL)`. Captures
// KEY and VAL so NiceUsage can reorder.
var sourcedDefaultRe = regexp.MustCompile(`\(sourced from ([^)]+)\) \(default ([^)]+)\)`)

// NiceUsage prints fs's usage with config-sourced defaults in
// `(default X, sourced from Y)` order. Wire it as fs.Usage:
//
//	fs.Usage = func() { cliutil.NiceUsage(fs) }
func NiceUsage(fs *flag.FlagSet) {
	var buf bytes.Buffer
	orig := fs.Output()
	fs.SetOutput(&buf)
	fmt.Fprintf(&buf, "Usage of %s:\n", fs.Name())
	fs.PrintDefaults()
	fs.SetOutput(orig)
	rewritten := sourcedDefaultRe.ReplaceAllString(buf.String(), "(default $2, sourced from $1)")
	fmt.Fprint(orig, rewritten)
}
