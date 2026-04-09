package userclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
	"github.com/steipete/discrawl/internal/discord"
)

const (
	gatewayURL     = "wss://gateway.discord.gg/?encoding=json&v=10"
	opDispatch     = 0
	opHeartbeat    = 1
	opIdentify     = 2
	opResume       = 6
	opReconnect    = 7
	opInvalidSess  = 9
	opHello        = 10
	opHeartbeatACK = 11
)

// gatewayPayload is the top-level Gateway frame.
type gatewayPayload struct {
	Op       int             `json:"op"`
	Data     json.RawMessage `json:"d"`
	Sequence *int64          `json:"s,omitempty"`
	Type     string          `json:"t,omitempty"`
}

// helloData is the payload of op 10 (HELLO).
type helloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

// readyData captures fields we need from the READY event.
type readyData struct {
	SessionID        string `json:"session_id"`
	ResumeGatewayURL string `json:"resume_gateway_url"`
}

// identifyPayload is the user-mode Identify (op 2).
type identifyPayload struct {
	Token        string              `json:"token"`
	Capabilities int                 `json:"capabilities"`
	Properties   json.RawMessage     `json:"properties"`
	Presence     identifyPresence    `json:"presence"`
	Compress     bool                `json:"compress"`
	ClientState  identifyClientState `json:"client_state"`
}

type identifyPresence struct {
	Status     string `json:"status"`
	Since      int    `json:"since"`
	Activities []any  `json:"activities"`
	AFK        bool   `json:"afk"`
}

type identifyClientState struct {
	GuildVersions          map[string]string `json:"guild_versions"`
	HighestLastMessageID   string            `json:"highest_last_message_id"`
	ReadStateVersion       int               `json:"read_state_version"`
	UserGuildSettingsVer   int               `json:"user_guild_settings_version"`
	UserSettingsVersion    int               `json:"user_settings_version"`
	PrivateChannelsVersion string            `json:"private_channels_version"`
	APICodeVersion         int               `json:"api_code_version"`
}

// Gateway handles the Discord Gateway WebSocket connection for user tokens.
type Gateway struct {
	token      string
	superProps string // base64 encoded, same as REST
	logger     *slog.Logger

	conn      *websocket.Conn
	mu        sync.Mutex // protects writes to conn
	seq       int64
	sessionID string
	resumeURL string

	handler discord.EventHandler
}

// NewGateway creates a Gateway client for user-token mode.
func NewGateway(token, superPropsEncoded string, logger *slog.Logger) *Gateway {
	if logger == nil {
		logger = slog.Default()
	}
	return &Gateway{
		token:      token,
		superProps: superPropsEncoded,
		logger:     logger,
	}
}

// Run connects to the Gateway and dispatches events to the handler until
// ctx is cancelled or a fatal error occurs.
func (g *Gateway) Run(ctx context.Context, handler discord.EventHandler) error {
	if handler == nil {
		return fmt.Errorf("missing event handler")
	}
	g.handler = handler

	url := gatewayURL
	for {
		err := g.session(ctx, url)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			g.logger.Warn("gateway session ended", "err", err)
		}
		// Try to resume
		if g.sessionID != "" && g.resumeURL != "" {
			url = g.resumeURL + "/?encoding=json&v=10"
			g.logger.Info("attempting gateway resume", "session_id", g.sessionID, "seq", g.seq)
		} else {
			url = gatewayURL
			g.sessionID = ""
			g.seq = 0
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
		}
	}
}

// session runs a single WebSocket connection lifecycle.
func (g *Gateway) session(ctx context.Context, url string) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}
	g.conn = conn
	defer func() {
		_ = conn.Close()
		g.conn = nil
	}()

	// Read HELLO (op 10).
	hello, err := g.readRawPayload(conn)
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if hello.Op != opHello {
		return fmt.Errorf("expected op 10 HELLO, got op %d", hello.Op)
	}
	var hd helloData
	if err := json.Unmarshal(hello.Data, &hd); err != nil {
		return fmt.Errorf("parse hello: %w", err)
	}

	// Send IDENTIFY or RESUME
	if g.sessionID != "" {
		if err := g.sendResume(conn); err != nil {
			return fmt.Errorf("send resume: %w", err)
		}
	} else {
		if err := g.sendIdentify(conn); err != nil {
			return fmt.Errorf("send identify: %w", err)
		}
	}

	// Start heartbeat
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()
	go g.heartbeatLoop(heartbeatCtx, conn, time.Duration(hd.HeartbeatInterval)*time.Millisecond)

	// Read loop
	for {
		if ctx.Err() != nil {
			return nil
		}
		payload, err := g.readRawPayload(conn)
		if err != nil {
			return fmt.Errorf("read payload: %w", err)
		}
		if err := g.handlePayload(ctx, conn, payload); err != nil {
			return err
		}
	}
}

func (g *Gateway) handlePayload(ctx context.Context, conn *websocket.Conn, p gatewayPayload) error {
	if p.Sequence != nil {
		g.seq = *p.Sequence
	}

	switch p.Op {
	case opDispatch:
		return g.dispatchEvent(ctx, p)
	case opHeartbeat:
		return g.sendHeartbeat(conn)
	case opReconnect:
		return fmt.Errorf("server requested reconnect")
	case opInvalidSess:
		var resumable bool
		_ = json.Unmarshal(p.Data, &resumable)
		if !resumable {
			g.sessionID = ""
			g.seq = 0
		}
		return fmt.Errorf("invalid session (resumable=%v)", resumable)
	case opHeartbeatACK:
		return nil
	default:
		return nil
	}
}

func (g *Gateway) dispatchEvent(ctx context.Context, p gatewayPayload) error {
	switch p.Type {
	case "READY":
		var ready readyData
		if err := json.Unmarshal(p.Data, &ready); err != nil {
			return fmt.Errorf("parse READY: %w", err)
		}
		g.sessionID = ready.SessionID
		g.resumeURL = ready.ResumeGatewayURL
		g.logger.Info("gateway READY", "session_id", g.sessionID)
		return nil

	case "RESUMED":
		g.logger.Info("gateway RESUMED")
		return nil

	case "MESSAGE_CREATE":
		var evt discordgo.MessageCreate
		if err := json.Unmarshal(p.Data, &evt); err != nil {
			return nil
		}
		return g.handler.OnMessageCreate(ctx, evt.Message)

	case "MESSAGE_UPDATE":
		var evt discordgo.MessageUpdate
		if err := json.Unmarshal(p.Data, &evt); err != nil {
			return nil
		}
		return g.handler.OnMessageUpdate(ctx, evt.Message)

	case "MESSAGE_DELETE":
		var evt discordgo.MessageDelete
		if err := json.Unmarshal(p.Data, &evt); err != nil {
			return nil
		}
		return g.handler.OnMessageDelete(ctx, &evt)

	case "CHANNEL_CREATE", "CHANNEL_UPDATE":
		var ch discordgo.Channel
		if err := json.Unmarshal(p.Data, &ch); err != nil {
			return nil
		}
		return g.handler.OnChannelUpsert(ctx, &ch)

	case "GUILD_MEMBER_ADD":
		var member discordgo.Member
		if err := json.Unmarshal(p.Data, &member); err != nil {
			return nil
		}
		return g.handler.OnMemberUpsert(ctx, member.GuildID, &member)

	case "GUILD_MEMBER_UPDATE":
		var data struct {
			GuildID  string          `json:"guild_id"`
			Nick     string          `json:"nick"`
			Avatar   string          `json:"avatar"`
			Roles    []string        `json:"roles"`
			JoinedAt time.Time       `json:"joined_at"`
			User     *discordgo.User `json:"user"`
		}
		if err := json.Unmarshal(p.Data, &data); err != nil {
			return nil
		}
		member := &discordgo.Member{
			GuildID:  data.GuildID,
			Nick:     data.Nick,
			Avatar:   data.Avatar,
			Roles:    data.Roles,
			JoinedAt: data.JoinedAt,
			User:     data.User,
		}
		return g.handler.OnMemberUpsert(ctx, data.GuildID, member)

	case "GUILD_MEMBER_REMOVE":
		var data struct {
			GuildID string          `json:"guild_id"`
			User    *discordgo.User `json:"user"`
		}
		if err := json.Unmarshal(p.Data, &data); err != nil || data.User == nil {
			return nil
		}
		return g.handler.OnMemberDelete(ctx, data.GuildID, data.User.ID)

	case "MESSAGE_REACTION_ADD":
		if rh, ok := g.handler.(discord.ReactionHandler); ok {
			var evt discord.ReactionEvent
			if err := json.Unmarshal(p.Data, &evt); err != nil {
				return nil
			}
			return rh.OnReactionAdd(ctx, &evt)
		}
		return nil

	case "MESSAGE_REACTION_REMOVE":
		if rh, ok := g.handler.(discord.ReactionHandler); ok {
			var evt discord.ReactionEvent
			if err := json.Unmarshal(p.Data, &evt); err != nil {
				return nil
			}
			return rh.OnReactionRemove(ctx, &evt)
		}
		return nil

	default:
		return nil
	}
}

func (g *Gateway) sendIdentify(conn *websocket.Conn) error {
	propsJSON, err := decodeSuperProps(g.superProps)
	if err != nil {
		return fmt.Errorf("decode super properties: %w", err)
	}

	identify := identifyPayload{
		Token:        g.token,
		Capabilities: 16381,
		Properties:   propsJSON,
		Presence: identifyPresence{
			Status:     "online",
			Since:      0,
			Activities: []any{},
			AFK:        false,
		},
		Compress: false,
		ClientState: identifyClientState{
			GuildVersions:          map[string]string{},
			HighestLastMessageID:   "0",
			ReadStateVersion:       0,
			UserGuildSettingsVer:   -1,
			UserSettingsVersion:    -1,
			PrivateChannelsVersion: "0",
			APICodeVersion:         0,
		},
	}

	payload := map[string]any{
		"op": opIdentify,
		"d":  identify,
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return conn.WriteJSON(payload)
}

func (g *Gateway) sendResume(conn *websocket.Conn) error {
	payload := map[string]any{
		"op": opResume,
		"d": map[string]any{
			"token":      g.token,
			"session_id": g.sessionID,
			"seq":        g.seq,
		},
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return conn.WriteJSON(payload)
}

func (g *Gateway) sendHeartbeat(conn *websocket.Conn) error {
	payload := map[string]any{
		"op": opHeartbeat,
		"d":  g.seq,
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return conn.WriteJSON(payload)
}

func (g *Gateway) heartbeatLoop(ctx context.Context, conn *websocket.Conn, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := g.sendHeartbeat(conn); err != nil {
				g.logger.Warn("heartbeat send failed", "err", err)
				return
			}
		}
	}
}

func (g *Gateway) readRawPayload(conn *websocket.Conn) (gatewayPayload, error) {
	var p gatewayPayload
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(msg, &p); err != nil {
		return p, fmt.Errorf("unmarshal gateway payload: %w", err)
	}
	return p, nil
}

func decodeSuperProps(encoded string) (json.RawMessage, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// TODO: zlib-stream compression support can be added later for bandwidth
// optimization. For now we use uncompressed JSON which is simpler and
// sufficient for personal archiving use cases.
