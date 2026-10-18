package memory

import (
	"fmt"
	"strings"
	"time"
)

// Timestamps in the discrawl schema are TEXT written by different producers
// (Discord payloads with microseconds and "+00:00", the native writer with
// RFC3339Nano "Z", markers with whole seconds). Comparing them as TEXT is
// wrong as soon as fractions or offsets differ ('...00.500Z' sorts BEFORE
// '...00Z'), so every temporal decision of the reconciliation (delta boundary
// and newest-wins) goes through parseInstant and compares real instants.

var instantLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
}

// parseInstant returns the instant of a stored timestamp. ok=false for NULL /
// empty values; unparseable non-empty values return ok=false and bad=true.
func parseInstant(v any) (t time.Time, ok, bad bool) {
	var s string
	switch x := v.(type) {
	case nil:
		return time.Time{}, false, false
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprint(x)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false, false
	}
	for _, l := range instantLayouts {
		if p, err := time.Parse(l, s); err == nil {
			return p.UTC(), true, false
		}
	}
	return time.Time{}, false, true
}

// atOrAfter reports whether a stored timestamp is at/after the boundary.
// Unparseable non-empty values are treated as touched (conservative: a
// superset of the delta is harmless because the upsert is newest-wins and
// idempotent, a missed row is a loss).
func atOrAfter(v any, bound time.Time) bool {
	t, ok, bad := parseInstant(v)
	if bad {
		return true
	}
	return ok && !t.Before(bound)
}

// strictlyNewer reports whether candidate is a strictly newer version than
// current (newest-wins). Equal instants written with different precision are
// NOT newer, which keeps a second run idempotent. When either side cannot be
// parsed the legacy TEXT order is used as a last resort.
func strictlyNewer(candidate, current any) bool {
	c, cok, cbad := parseInstant(candidate)
	a, aok, abad := parseInstant(current)
	switch {
	case cok && aok:
		return c.After(a)
	case cok && !aok && !abad:
		return true // archive has no timestamp at all
	case !cok && !cbad:
		return false // candidate has no timestamp: never overwrite
	default:
		return fmt.Sprint(candidate) > fmt.Sprint(current)
	}
}
