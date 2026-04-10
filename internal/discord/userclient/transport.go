package userclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	cryptorand "crypto/rand"
	"encoding/hex"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

const discordAPIBase = "https://discord.com/api/v10"

// superProperties is the X-Super-Properties JSON payload that mimics a real
// Discord web client. Fields must stay in sync with what the client sends.
type superProperties struct {
	OS                        string `json:"os"`
	Browser                   string `json:"browser"`
	Device                    string `json:"device"`
	SystemLocale              string `json:"system_locale"`
	HasClientMods             bool   `json:"has_client_mods"`
	BrowserUserAgent          string `json:"browser_user_agent"`
	BrowserVersion            string `json:"browser_version"`
	OSVersion                 string `json:"os_version"`
	Referrer                  string `json:"referrer"`
	ReferringDomain           string `json:"referring_domain"`
	SearchEngine              string `json:"search_engine"`
	ReferrerCurrent           string `json:"referrer_current"`
	ReferringDomainCurrent    string `json:"referring_domain_current"`
	ReleaseChannel            string `json:"release_channel"`
	ClientBuildNumber         int    `json:"client_build_number"`
	ClientEventSource         *int   `json:"client_event_source"`
	ClientLaunchID            string `json:"client_launch_id"`
	LaunchSignature           string `json:"launch_signature"`
	ClientHeartbeatSessionID  string `json:"client_heartbeat_session_id"`
	ClientAppState            string `json:"client_app_state"`
}

// transportConfig holds the immutable configuration for the HTTP transport.
type transportConfig struct {
	token             string
	userAgent         string
	locale            string
	superPropsEncoded string
	minGap            time.Duration
	jitterMin         time.Duration
	jitterMax         time.Duration
	readOnlyStrict    bool
	proxy             string
}

// transport is the HTTP transport layer for user-token Discord requests.
// It handles headers, rate limiting, and read-only enforcement.
type transport struct {
	cfg    transportConfig
	client *http.Client

	mu       sync.Mutex
	lastReq  time.Time
}

func newTransport(cfg transportConfig) *transport {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	// TODO: proxy support via cfg.proxy (SOCKS5/HTTP)
	return &transport{
		cfg:    cfg,
		client: httpClient,
	}
}

func buildSuperProperties(userAgent, browserVersion, locale string, buildNumber int) string {
	props := superProperties{
		OS:                       "Mac OS X",
		Browser:                  "Chrome",
		Device:                   "",
		SystemLocale:             locale,
		HasClientMods:            false,
		BrowserUserAgent:         userAgent,
		BrowserVersion:           browserVersion,
		OSVersion:                "10.15.7",
		Referrer:                 "https://www.google.com/",
		ReferringDomain:          "www.google.com",
		SearchEngine:             "google",
		ReferrerCurrent:          "https://discord.com/",
		ReferringDomainCurrent:   "discord.com",
		ReleaseChannel:           "stable",
		ClientBuildNumber:        buildNumber,
		ClientEventSource:        nil,
		ClientLaunchID:           generateUUID(),
		LaunchSignature:          generateUUID(),
		ClientHeartbeatSessionID: generateUUID(),
		ClientAppState:           "focused",
	}
	data, _ := json.Marshal(props)
	return base64.StdEncoding.EncodeToString(data)
}

func generateUUID() string {
	var buf [16]byte
	_, _ = cryptorand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(buf[0:4]),
		hex.EncodeToString(buf[4:6]),
		hex.EncodeToString(buf[6:8]),
		hex.EncodeToString(buf[8:10]),
		hex.EncodeToString(buf[10:16]))
}

// do executes an HTTP request with all user-mode headers and rate limiting.
func (t *transport) do(ctx context.Context, method, path string, body *strings.Reader) (*http.Response, error) {
	if t.cfg.readOnlyStrict && method != "GET" {
		return nil, fmt.Errorf("discrawl-me: read-only mode blocks %s %s", method, path)
	}

	// Rate limiting: wait for minimum gap + jitter
	t.throttle()

	url := discordAPIBase + path
	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequestWithContext(ctx, method, url, body)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	}
	if err != nil {
		return nil, err
	}

	// User-mode headers — must match a real Discord web client
	req.Header.Set("Authorization", t.cfg.token) // no "Bot " prefix
	req.Header.Set("User-Agent", t.cfg.userAgent)
	req.Header.Set("X-Super-Properties", t.cfg.superPropsEncoded)
	req.Header.Set("X-Discord-Locale", t.cfg.locale)
	req.Header.Set("X-Discord-Timezone", "Europe/Rome")
	req.Header.Set("X-Debug-Options", "bugReporterEnabled")
	req.Header.Set("Accept-Language", t.cfg.locale+",en;q=0.9")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/channels/@me")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Ch-Ua", `"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"macOS"`)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}

	// Handle 429 rate limit
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp)
		_ = resp.Body.Close()
		if retryAfter > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryAfter + t.jitter()):
			}
			return t.do(ctx, method, path, body) // retry once
		}
		return nil, fmt.Errorf("rate limited on %s %s with no retry-after", method, path)
	}

	// Auth failures: 401 is fatal (token invalid), 403 is recoverable (missing access)
	if resp.StatusCode == http.StatusUnauthorized {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("discord auth error 401 on %s %s — token is invalid or account suspended", method, path)
	}
	if resp.StatusCode == http.StatusForbidden {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("discord auth error 403 on %s %s — missing access to this resource", method, path)
	}

	return resp, nil
}

// throttle enforces the minimum gap between requests with jitter.
func (t *transport) throttle() {
	t.mu.Lock()
	defer t.mu.Unlock()

	elapsed := time.Since(t.lastReq)
	needed := t.cfg.minGap + t.jitter()
	if elapsed < needed {
		time.Sleep(needed - elapsed)
	}
	t.lastReq = time.Now()
}

// jitter returns a random duration between jitterMin and jitterMax.
func (t *transport) jitter() time.Duration {
	if t.cfg.jitterMax <= t.cfg.jitterMin {
		return t.cfg.jitterMin
	}
	delta := t.cfg.jitterMax - t.cfg.jitterMin
	return t.cfg.jitterMin + time.Duration(rand.Int64N(int64(delta)))
}

func parseRetryAfter(resp *http.Response) time.Duration {
	var body struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0
	}
	if body.RetryAfter > 0 {
		return time.Duration(body.RetryAfter*1000) * time.Millisecond
	}
	return 0
}
