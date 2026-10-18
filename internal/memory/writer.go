package memory

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// WriterRetention adapts a Retainer to the store write hook used by the
// native collector (sync/tail) on a selective DB. Decisions use the catalog
// stored in the same DB; an unknown channel triggers a catalog reload.
type WriterRetention struct {
	mu  sync.Mutex
	db  *sql.DB
	ret *Retainer
	// rollback window state read at install time (see rollback_window.go).
	holdOpen   bool
	boundary   time.Time
	boundaryOK bool
}

// InstallWriterRetention validates the DB marker and the policy, then installs
// the opt-in write policy on s. Without the marker it refuses: the legacy
// writer stays unchanged for full archives.
func InstallWriterRetention(ctx context.Context, s *store.Store, policyPath string, now time.Time) (*WriterRetention, error) {
	m, err := ReadMarker(ctx, s.DB())
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("--memory-policy refused: DB is not a selective memory (missing marker)")
	}
	pol, err := LoadPolicy(policyPath, now)
	if err != nil {
		return nil, err
	}
	ret, err := LoadRetainer(ctx, s.DB(), pol)
	if err != nil {
		return nil, err
	}
	if err := ret.ValidateCatalog(); err != nil {
		return nil, err
	}
	w := &WriterRetention{db: s.DB(), ret: ret}
	// Rollback window (decision A): while open, records created/edited/
	// deleted at/after the boundary are accepted even if the rolling policy
	// would skip them, so the post-boundary delta exists for reconcile.
	if open, _ := windowState(m); open {
		w.holdOpen = true
		if b, err := markerBoundary(m); err == nil {
			w.boundary, w.boundaryOK = b, true
		}
	}
	s.SetMessageRetention(w)
	return w, nil
}

// rollbackHold reports whether an otherwise-skipped record must be kept
// because it carries a change at/after the boundary of an open window.
// Out-of-scope guilds are never kept. With an unusable boundary any edited
// or deleted record is kept (conservative); creation alone is not.
func (w *WriterRetention) rollbackHold(rec store.MessageRecord, reason string) bool {
	if !w.holdOpen || reason == ReasonOutOfScope {
		return false
	}
	if !w.boundaryOK {
		return rec.EditedAt != "" || rec.DeletedAt != ""
	}
	for _, at := range []string{rec.CreatedAt, rec.EditedAt, rec.DeletedAt} {
		if at != "" && atOrAfter(at, w.boundary) {
			return true
		}
	}
	return false
}

// RetainMessage implements store.MessageRetention.
func (w *WriterRetention) RetainMessage(rec store.MessageRecord) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.ret.Channels[rec.ChannelID]; !ok {
		if fresh, err := LoadRetainer(context.Background(), w.db, w.ret.P); err == nil {
			w.ret = fresh
		}
	}
	ok, reason := w.ret.Decide(rec.GuildID, rec.ChannelID, rec.ID, rec.CreatedAt)
	return ok || w.rollbackHold(rec, reason)
}

// SinceFloor bounds backfill per channel: INFO rooms unbounded, pinned
// channels from their registry since, everything else from the rolling cutoff.
// Bounded backfill stops at the floor without declaring history_complete.
func (w *WriterRetention) SinceFloor(channelID string) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ret.P.InfoEnabled() {
		if w.ret.InfoNow[channelID] {
			return time.Time{}
		}
		if _, seen := w.ret.InfoSeen[channelID]; seen {
			return time.Time{}
		}
	}
	if pin, ok := w.ret.P.Pins[channelID]; ok && pin.Since.Before(w.ret.P.Cutoff) {
		return pin.Since
	}
	return w.ret.P.Cutoff
}
