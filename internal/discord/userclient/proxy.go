package userclient

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	xproxy "golang.org/x/net/proxy"
)

// proxyConfig carries the two different shapes an outbound proxy can take, so
// that the REST client and the WebSocket dialer can both be wired from one
// parse. Exactly one of the fields is set, or neither when connecting directly.
//
// Why two shapes: an HTTP proxy is spoken *inside* the connection (CONNECT),
// which both net/http and gorilla/websocket already know how to do, so it is
// enough to hand them the URL. A SOCKS5 proxy is spoken *before* the
// connection exists, so it has to replace the dial itself.
type proxyConfig struct {
	// dialContext replaces the direct TCP dial. Set for socks5 schemes.
	dialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	// proxyURL is handed to net/http and gorilla as a CONNECT proxy.
	// Set for http and https schemes.
	proxyURL *url.URL
}

// proxyFunc returns the value for http.Transport.Proxy and
// websocket.Dialer.Proxy.
//
// The fallback matters: before this file existed, the transport used
// http.DefaultTransport and the gateway used websocket.DefaultDialer, and both
// of those honour HTTP_PROXY/HTTPS_PROXY/NO_PROXY. Anyone relying on the
// environment would silently lose their proxy the day we started building our
// own transport, and a self-bot that silently stops using a proxy is exactly
// the failure we cannot afford. So: an explicit config wins, and the
// environment still applies when there is none.
func (p *proxyConfig) proxyFunc() func(*http.Request) (*url.URL, error) {
	if p == nil || p.proxyURL == nil {
		return http.ProxyFromEnvironment
	}
	return http.ProxyURL(p.proxyURL)
}

// newProxyConfig parses the `proxy` config value.
//
// An empty value means "no explicit proxy": the environment still applies, as
// it always has. Supported schemes are socks5, socks5h, http and https. A bare
// host:port is read as socks5, because that is the only scheme that needed new
// code here and therefore the only reason to be setting this at all.
//
// Note on DNS: for socks5 we always pass the hostname to the proxy rather than
// resolving it locally (what curl spells socks5h). For a user-token client
// that is not a detail — resolving discord.com from the machine running the
// client would put that machine's resolver, and its network, back in the path
// we are trying to keep out of it.
func newProxyConfig(raw string) (*proxyConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &proxyConfig{}, nil
	}

	if !strings.Contains(raw, "://") {
		raw = "socks5://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse proxy %q: %w", raw, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("proxy %q has no host", raw)
	}

	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return &proxyConfig{proxyURL: u}, nil

	case "socks5", "socks5h":
		var auth *xproxy.Auth
		if u.User != nil {
			pass, _ := u.User.Password()
			auth = &xproxy.Auth{User: u.User.Username(), Password: pass}
		}
		// The forward dialer carries the timeout: x/net/proxy dials with
		// Dial, which has no context, so without this a dead proxy would
		// hang the caller for as long as the OS allows.
		forward := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
		d, err := xproxy.SOCKS5("tcp", u.Host, auth, forward)
		if err != nil {
			return nil, fmt.Errorf("socks5 proxy %s: %w", u.Host, err)
		}
		cd, ok := d.(xproxy.ContextDialer)
		if !ok {
			// x/net/proxy's SOCKS5 dialer has implemented ContextDialer for
			// years. If that ever stops being true we want to hear about it
			// at startup, not to quietly lose context cancellation.
			return nil, fmt.Errorf("socks5 proxy %s: dialer does not support contexts", u.Host)
		}
		return &proxyConfig{dialContext: cd.DialContext}, nil

	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q (want socks5, socks5h, http or https)", u.Scheme)
	}
}
