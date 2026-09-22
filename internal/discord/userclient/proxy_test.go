package userclient

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- a minimal SOCKS5 server, enough to prove a connection really goes
// through it and to record what the client asked it to reach. ---

type socksProbe struct {
	ln net.Listener
	// requested is the address the client asked the proxy to connect to, in
	// the form the client sent it: a hostname stays a hostname.
	mu        sync.Mutex
	requested []string
	conns     int
}

func newSocksProbe(t *testing.T) *socksProbe {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &socksProbe{ln: ln}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *socksProbe) addr() string { return p.ln.Addr().String() }

func (p *socksProbe) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.requested...)
}

func (p *socksProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conns
}

func (p *socksProbe) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *socksProbe) handle(c net.Conn) {
	defer c.Close()

	// Greeting: version, nmethods, methods...
	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil || head[0] != 0x05 {
		return
	}
	if _, err := io.ReadFull(c, make([]byte, int(head[1]))); err != nil {
		return
	}
	// "no authentication required"
	if _, err := c.Write([]byte{0x05, 0x00}); err != nil {
		return
	}

	// Request: version, cmd, reserved, atyp
	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil || req[1] != 0x01 { // CONNECT
		return
	}

	var host string
	switch req[3] {
	case 0x01: // IPv4
		b := make([]byte, 4)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = net.IP(b).String()
	case 0x03: // domain name — what socks5h semantics produce
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return
		}
		b := make([]byte, int(l[0]))
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = string(b)
	case 0x04: // IPv6
		b := make([]byte, 16)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = net.IP(b).String()
	default:
		return
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(c, pb); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(pb)
	target := net.JoinHostPort(host, strconv.Itoa(int(port)))

	p.mu.Lock()
	p.requested = append(p.requested, target)
	p.conns++
	p.mu.Unlock()

	up, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		_, _ = c.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer up.Close()
	// success, bound address 0.0.0.0:0 — clients ignore it for CONNECT
	if _, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}

	done := make(chan struct{})
	go func() { _, _ = io.Copy(up, c); close(done) }()
	_, _ = io.Copy(c, up)
	<-done
}

// --- parsing ---

func TestNewProxyConfigEmptyMeansEnvironment(t *testing.T) {
	// The invariant that protects existing installs: before this existed the
	// client used http.DefaultTransport and websocket.DefaultDialer, both of
	// which read HTTP_PROXY. An empty setting must not take that away.
	pc, err := newProxyConfig("")
	require.NoError(t, err)
	require.Nil(t, pc.dialContext)
	require.Nil(t, pc.proxyURL)

	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9999")
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
	require.NoError(t, err)
	u, err := pc.proxyFunc()(req)
	require.NoError(t, err)
	require.NotNil(t, u, "an empty proxy setting must still honour HTTP_PROXY")
	require.Equal(t, "127.0.0.1:9999", u.Host)
}

func TestNewProxyConfigSchemes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		wantSOC bool
		wantURL string
	}{
		{"socks5", "socks5://127.0.0.1:1080", true, ""},
		{"socks5h", "socks5h://127.0.0.1:1080", true, ""},
		{"socks5 with auth", "socks5://u:p@127.0.0.1:1080", true, ""},
		{"bare host:port defaults to socks5", "100.123.71.99:1080", true, ""},
		{"http", "http://127.0.0.1:3128", false, "127.0.0.1:3128"},
		{"https", "https://proxy.example:8443", false, "proxy.example:8443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc, err := newProxyConfig(tc.raw)
			require.NoError(t, err)
			if tc.wantSOC {
				require.NotNil(t, pc.dialContext, "socks5 must replace the dial")
				require.Nil(t, pc.proxyURL, "socks5 is not a CONNECT proxy")
				return
			}
			require.Nil(t, pc.dialContext, "http proxies keep the normal dial")
			require.NotNil(t, pc.proxyURL)
			require.Equal(t, tc.wantURL, pc.proxyURL.Host)
		})
	}
}

func TestNewProxyConfigRejectsBadValues(t *testing.T) {
	// A misconfigured proxy has to be a startup error. Falling back to a
	// direct connection would send the user token out from whatever IP the
	// machine happens to have, which is the single outcome this feature
	// exists to prevent.
	for _, tc := range []struct{ name, raw, wantErr string }{
		{"unknown scheme", "ftp://127.0.0.1:21", "unsupported proxy scheme"},
		{"scheme without host", "socks5://", "no host"},
		{"not a url", "socks5://%zz", "parse proxy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc, err := newProxyConfig(tc.raw)
			require.Error(t, err)
			require.Nil(t, pc)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestNewRejectsBadProxyInsteadOfGoingDirect(t *testing.T) {
	cfg := testConfig()
	cfg.Proxy = "ftp://127.0.0.1:21"
	client, err := New("test-token", cfg)
	require.Error(t, err, "a bad proxy must fail the client, not be ignored")
	require.Nil(t, client)
}

// --- the connection really goes through the proxy ---

func TestRESTTrafficTraversesSOCKS5(t *testing.T) {
	var hits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	probe := newSocksProbe(t)
	pc, err := newProxyConfig("socks5://" + probe.addr())
	require.NoError(t, err)

	tr := newTransport(transportConfig{}, pc)
	resp, err := tr.client.Get(upstream.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, "ok", string(body))
	require.Equal(t, 1, hits, "the upstream must have been reached")
	require.Equal(t, 1, probe.count(), "and it must have been reached through the proxy")

	u, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	require.Equal(t, []string{u.Host}, probe.seen())
}

func TestSOCKS5ResolvesHostnamesAtTheProxy(t *testing.T) {
	// socks5h semantics. If the name were resolved locally the proxy would
	// see an IP here, and the DNS query would have come from the machine we
	// are trying to keep out of the path.
	probe := newSocksProbe(t)
	pc, err := newProxyConfig("socks5://" + probe.addr())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The target does not resolve anywhere; we only care what was sent.
	conn, err := pc.dialContext(ctx, "tcp", "gateway.discord.gg:443")
	if err == nil {
		_ = conn.Close()
	}

	seen := probe.seen()
	require.Len(t, seen, 1)
	require.Equal(t, "gateway.discord.gg:443", seen[0],
		"the hostname must be handed to the proxy, not resolved locally")
}

func TestGatewayDialerUsesTheProxy(t *testing.T) {
	probe := newSocksProbe(t)
	pc, err := newProxyConfig("socks5://" + probe.addr())
	require.NoError(t, err)

	g := NewGateway("test-token", "props", nil, pc)
	require.NotNil(t, g.dialer)
	require.NotNil(t, g.dialer.NetDialContext, "the websocket dial must go through socks5 too")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// No websocket server behind it: the handshake will fail, but only after
	// the proxy has been asked to make the connection, which is the point.
	_, _, _ = g.dialer.DialContext(ctx, "wss://gateway.discord.gg/?v=10", nil)

	require.NotEmpty(t, probe.seen(), "the gateway dial must reach the proxy")
	require.True(t, strings.HasPrefix(probe.seen()[0], "gateway.discord.gg:"),
		fmt.Sprintf("unexpected target %q", probe.seen()[0]))
}

func TestGatewayWithoutProxyStillHonoursEnvironment(t *testing.T) {
	pc, err := newProxyConfig("")
	require.NoError(t, err)
	g := NewGateway("test-token", "props", nil, pc)
	require.NotNil(t, g.dialer)
	require.Nil(t, g.dialer.NetDialContext, "no socks5 means the normal dial")
	require.NotNil(t, g.dialer.Proxy, "but HTTP_PROXY must still be read")
}
