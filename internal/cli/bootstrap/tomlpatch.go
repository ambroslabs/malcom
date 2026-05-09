// Line-based TOML editor for the few keys malcom needs to override
// after `<chain-exe> init` writes the config files. We don't want a
// full TOML round-trip — that loses comments and reformats the file,
// which downstream operators rely on for hand-editing.
//
// The patcher walks lines, tracks the current `[section]`, and on a
// matching `key = ...` line in the target section replaces the value
// while preserving leading whitespace and any inline comment.

package bootstrap

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

// setTOMLString sets <section>.<key> = "value" in the file at path.
// section == "" means the top-level (lines before any [section] header).
// Returns an error if the key is not present in section — we never
// invent keys, only replace existing ones.
func setTOMLString(path, section, key, value string) error {
	return patchTOMLKey(path, section, key, fmt.Sprintf("%q", value))
}

// setTOMLInt64 sets <section>.<key> = <n> in the file at path.
func setTOMLInt64(path, section, key string, n int64) error {
	return patchTOMLKey(path, section, key, fmt.Sprintf("%d", n))
}

// patchTOMLKey is the line-walker that backs the typed setters. The
// rawValue argument is inserted verbatim after `key = `; the caller
// is responsible for quoting/formatting.
func patchTOMLKey(path, section, key, rawValue string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	var out bytes.Buffer
	currentSection := ""
	matched := false

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Section header detection: a line whose first non-space char
		// is '[' and that ends with ']'. Doesn't handle inline-table
		// or array-of-tables syntax — fine, we don't write into those.
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") && !strings.HasPrefix(trimmed, "[[") {
			currentSection = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			out.WriteString(line)
			out.WriteByte('\n')
			continue
		}

		if !matched && currentSection == section && lineDeclaresKey(trimmed, key) {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			out.WriteString(indent)
			out.WriteString(key)
			out.WriteString(" = ")
			out.WriteString(rawValue)
			out.WriteByte('\n')
			matched = true
			continue
		}

		out.WriteString(line)
		out.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}

	if !matched {
		return fmt.Errorf("key %q not found in section %q of %s", key, section, path)
	}

	// Preserve the file's trailing-newline state: if the original
	// didn't end in newline, drop the one we appended on the last
	// line.
	final := out.Bytes()
	if len(body) > 0 && body[len(body)-1] != '\n' && len(final) > 0 && final[len(final)-1] == '\n' {
		final = final[:len(final)-1]
	}

	tmp := path + ".malcom.tmp"
	if err := os.WriteFile(tmp, final, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// lineDeclaresKey reports whether a (whitespace-trimmed) line declares
// the given key — i.e. starts with `key` followed by optional space
// and then `=`. Comments (lines starting with '#') don't count.
func lineDeclaresKey(trimmed, key string) bool {
	if trimmed == "" || trimmed[0] == '#' {
		return false
	}
	rest := strings.TrimPrefix(trimmed, key)
	if rest == trimmed {
		return false
	}
	rest = strings.TrimLeft(rest, " \t")
	return strings.HasPrefix(rest, "=")
}
