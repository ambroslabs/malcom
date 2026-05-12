// Package cliexit defines the typed error subcommands return from
// cobra.Command.RunE when they want a specific process exit code.
//
// cmd/malcom/main.go inspects the error from Command.Execute() via
// errors.As; on a hit it exits with the embedded Code, on a miss it
// prints the error and exits 2 (treated as a usage / flag-parse
// failure). Subcommands are responsible for emitting their own
// human-readable diagnostics (via slog) before returning Error — the
// type carries no message of its own to avoid double-printing.
package cliexit

import "fmt"

type Error struct {
	Code int
}

func (e *Error) Error() string {
	return fmt.Sprintf("exit code %d", e.Code)
}
