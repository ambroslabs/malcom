# Contributing to malcom

## Before sending a PR

- `go test ./... -race` should pass locally. CI also runs `go vet`,
  `staticcheck`, and `golangci-lint` — easier to fix findings
  locally than wait for a red CI.
- Format with `gofmt` (most editors do this for you).
- For non-trivial changes, open an issue first to discuss the
  approach. Avoids round trips on PRs whose direction wasn't
  agreed on.

## Commit and PR style

- Commit subject in the imperative ("fix the X bug", not "fixed the
  X bug"). Subject under 72 chars.
- Body explains *why* — the diff already shows the *what*.
- Reference the issue number when relevant: `(#118)` at the end of
  the subject, or `Closes #118` in the body.
- PR title mirrors the merge commit subject (we squash-merge).

## Code style

- One canonical way per concern: atomic writes go through
  `internal/durable`, not hand-rolled tmp+rename; logging goes
  through `internal/log` shims, not `os.Exit` or raw `fmt.Println`.
  Look at the package's existing pattern before adding a new one.
- Doc comments on exported (and non-trivial unexported) functions,
  types, and packages.
- Comments on the WHY, not the WHAT. The code shows what it does;
  comments capture invariants, gotchas, and design context that
  the diff alone won't preserve.

## License of contributions

By contributing you agree your contribution may be distributed
under either the Apache-2.0 or MIT license, matching the
dual-licensing of the project (see [README.md](README.md#license)).

No CLA is required.
