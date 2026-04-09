package userclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	OS                     string `json:"os"`
	Browser                string `json:"browser"`
	Device                 string `json:"device"`
	SystemLocale           string `json:"system_locale"`
	BrowserUserAgent       string `json:"browser_user_agent"`
	BrowserVersion         string `json:"browser_version"`
	OSVersion              string `json:"os_version"`
	Referrer               string `json:"referrer"`
	ReferringDomain        string `json:"referring_domain"`
	ReferrerCurrent        string `json:"referrer_current"`
	ReferringDomainCurrent string `json:"referring_domain_current"`
	ReleaseChannel         string `json:"release_channel"`
	ClientBuildNumber      int    `json:"client_build_number"`
	ClientEventSource      *int   `json:"client_event_source"`
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
		OS:                     "Mac OS X",
		Browser:                "Chrome",
		Device:                 "",
		SystemLocale:           locale,
		BrowserUserAgent:       userAgent,
		BrowserVersion:         browserVersion,
		OSVersion:              "10.15.7",
		Referrer:               "",
		ReferringDomain:        "",
		ReferrerCurrent:        "",
		ReferringDomainCurrent: "",
		ReleaseChannel:         "stable",
		ClientBuildNumber:      buildNumber,
		ClientEventSource:      nil,
	}
	data, _ := json.Marshal(props)
	return base64.StdEncoding.EncodeToString(data)
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

	// User-mode headers
	req.Header.Set("Authorization", t.cfg.token) // no "Bot " prefix
	req.Header.Set("User-Agent", t.cfg.userAgent)
	req.Header.Set("X-Super-Properties", t.cfg.superPropsEncoded)
	req.Header.Set("X-Discord-Locale", t.cfg.locale)
	req.Header.Set("X-Discord-Timezone", "America/New_York")
	req.Header.Set("Accept-Language", t.cfg.locale+",en;q=0.9")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/channels/@me")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Ch-Ua", `"Chromium";v="131", "Not_A Brand";v="24"`)
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

	// Hard stop on auth failures
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("discord auth error %d on %s %s — token may be invalid or account suspended", resp.StatusCode, method, path)
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
