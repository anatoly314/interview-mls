// Package hub fans status events out to connected websocket clients.
//
// Push is a hint, never the truth: a client that misses an event because it
// was reconnecting recovers by calling GET /jobs, so the hub is free to drop
// messages for a slow client rather than block the sender.
package hub

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// sendBuffer is how far behind a client may fall before it gets dropped.
	sendBuffer = 32
	// pings keep idle connections alive through proxies; pongWait must exceed
	// pingPeriod or every idle client would be reaped.
	pingPeriod = 30 * time.Second
	pongWait   = 70 * time.Second
	writeWait  = 10 * time.Second
)

// Event is the wire shape pushed to clients.
type Event struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type client struct {
	conn *websocket.Conn
	send chan Event
}

type Hub struct {
	log        *slog.Logger
	done       chan struct{}
	register   chan *client
	unregister chan *client
	broadcast  chan Event
	clients    map[*client]struct{}
	upgrader   websocket.Upgrader
}

func New(log *slog.Logger) *Hub {
	return &Hub{
		log:        log,
		done:       make(chan struct{}),
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan Event, 64),
		clients:    make(map[*client]struct{}),
		upgrader: websocket.Upgrader{
			// No auth by design: the demo has no notion of a user, so there is
			// nothing an origin check could protect.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// Run owns the client set; every mutation goes through these channels, so no
// lock is needed anywhere else.
func (h *Hub) Run() {
	for {
		select {
		case <-h.done:
			for c := range h.clients {
				close(c.send)
			}
			h.clients = nil
			return
		case c := <-h.register:
			h.clients[c] = struct{}{}
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
		case ev := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- ev:
				default:
					h.log.Warn("websocket client too slow, dropping event", "job_id", ev.JobID)
				}
			}
		}
	}
}

// Broadcast never blocks: the caller is the gRPC receive loop and a stalled
// hub must not back up worker status events.
func (h *Hub) Broadcast(ev Event) {
	select {
	case h.broadcast <- ev:
	case <-h.done:
	default:
		h.log.Warn("hub backlog full, dropping event", "job_id", ev.JobID)
	}
}

// Close stops Run and closes every registered client.
func (h *Hub) Close() { close(h.done) }

// ServeHTTP upgrades the request and runs the two pumps for its lifetime.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote the error response.
		h.log.Warn("websocket upgrade", "err", err)
		return
	}
	c := &client{conn: conn, send: make(chan Event, sendBuffer)}
	select {
	case h.register <- c:
	case <-h.done:
		_ = conn.Close()
		return
	}
	go c.writePump(h.log)
	c.readPump(h)
}

// readPump discards everything the client sends -- the protocol is one-way --
// but has to run anyway, because that is what processes pong and close frames.
func (c *client) readPump(h *Hub) {
	defer func() {
		select {
		case h.unregister <- c:
		case <-h.done:
		}
		_ = c.conn.Close()
	}()
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *client) writePump(log *slog.Logger) {
	t := time.NewTicker(pingPeriod)
	defer func() {
		t.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case ev, ok := <-c.send:
			if !ok {
				_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
					websocket.CloseNormalClosure, ""))
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteJSON(ev); err != nil {
				log.Warn("websocket write", "err", err)
				return
			}
		case <-t.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
