package memory

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteCLIFixture is an evidence helper, skipped unless
// DISCRAWL_MEMORY_FIXTURE_DIR is set: it writes the SYNTHETIC source DB,
// policy, registry and state so the real binary can be exercised offline.
func TestWriteCLIFixture(t *testing.T) {
	dst := os.Getenv("DISCRAWL_MEMORY_FIXTURE_DIR")
	if dst == "" {
		t.Skip("evidence helper only")
	}
	f, _ := buildFixture(t)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"source.db", "releases.json", "state.json"} {
		copyFile(t, filepath.Join(f.dir, name), filepath.Join(dst, name))
	}
	pol := defaultPolicy(f)
	pol["pins"] = map[string]any{"registry_path": filepath.Join(dst, "releases.json"), "state_path": filepath.Join(dst, "state.json")}
	writeJSON(t, filepath.Join(dst, "policy.json"), pol)
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}
