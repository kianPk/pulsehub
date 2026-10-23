package hub

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

type Message struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type"`
	Room    string `json:"room"`
	From    string `json:"from"`
	Text    string `json:"text,omitempty"`
	At      int64  `json:"at"`
	Members int    `json:"members,omitempty"`
}

type Client struct {
	id       string
	name     string
	room     string
	conn     *websocket.Conn
	send     chan []byte
	hub      *Hub
	lastChat time.Time
}

type Hub struct {
	mu        sync.RWMutex
	rooms     map[string]map[*Client]bool
	history   map[string][]Message
	tokens    map[string]string // room -> join token (optional)
	maxRoom   int
	chatGap   time.Duration
	histLimit int
	log       *log.Logger
}

func New(logger *log.Logger) *Hub {
	return &Hub{
		rooms:     map[string]map[*Client]bool{},
		history:   map[string][]Message{},
		tokens:    map[string]string{},
		maxRoom:   64,
		chatGap:   200 * time.Millisecond,
		histLimit: 150,
		log:       logger,
	}
}

func (h *Hub) SetRoomToken(room, token string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if token == "" {
		delete(h.tokens, room)
		return
	}
	h.tokens[room] = token
}

func (h *Hub) Stats() map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rooms := map[string]int{}
	clients := 0
	for name, set := range h.rooms {
		rooms[name] = len(set)
		clients += len(set)
	}
	return map[string]any{"rooms": rooms, "clients": clients, "history_limit": h.histLimit}
}

func (h *Hub) Rooms() []map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]map[string]any, 0, len(h.rooms))
	for name, set := range h.rooms {
		out = append(out, map[string]any{
			"name":    name,
			"members": len(set),
			"locked":  h.tokens[name] != "",
		})
	}
	return out
}

func (h *Hub) Presence(room string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	set := h.rooms[room]
	names := make([]string, 0, len(set))
	for c := range set {
		names = append(names, c.name)
	}
	return names
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		name = "anon"
	}
	if len(name) > 32 {
		name = name[:32]
	}
	room := strings.TrimSpace(r.URL.Query().Get("room"))
	if room == "" {
		room = "lobby"
	}
	if len(room) > 48 {
		room = room[:48]
	}
	token := r.URL.Query().Get("token")

	h.mu.RLock()
	need := h.tokens[room]
	members := len(h.rooms[room])
	h.mu.RUnlock()
	if need != "" && token != need {
		http.Error(w, "invalid room token", http.StatusUnauthorized)
		return
	}
	if members >= h.maxRoom {
		http.Error(w, "room full", http.StatusTooManyRequests)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &Client{
		id:   shortID(),
		name: name,
		room: room,
		conn: conn,
		send: make(chan []byte, 64),
		hub:  h,
	}
	h.join(c)
	go c.writePump()
	c.readPump()
}

func (h *Hub) join(c *Client) {
	h.mu.Lock()
	if h.rooms[c.room] == nil {
		h.rooms[c.room] = map[*Client]bool{}
	}
	h.rooms[c.room][c] = true
	hist := append([]Message{}, h.history[c.room]...)
	members := len(h.rooms[c.room])
	h.mu.Unlock()

	for _, m := range hist {
		b, _ := json.Marshal(m)
		select {
		case c.send <- b:
		default:
		}
	}
	roster, _ := json.Marshal(Message{
		Type:    "roster",
		Room:    c.room,
		From:    "system",
		Text:    strings.Join(h.Presence(c.room), ","),
		At:      time.Now().UnixMilli(),
		Members: members,
	})
	select {
	case c.send <- roster:
	default:
	}
	h.broadcast(c.room, Message{
		Type:    "presence",
		Room:    c.room,
		From:    c.name,
		Text:    "joined",
		At:      time.Now().UnixMilli(),
		Members: members,
	}, nil)
	h.log.Printf("%s joined #%s (%d)", c.name, c.room, members)
}

func (h *Hub) leave(c *Client) {
	h.mu.Lock()
	if set := h.rooms[c.room]; set != nil {
		delete(set, c)
		if len(set) == 0 {
			delete(h.rooms, c.room)
		}
	}
	members := 0
	if set := h.rooms[c.room]; set != nil {
		members = len(set)
	}
	h.mu.Unlock()
	close(c.send)
	h.broadcast(c.room, Message{
		Type:    "presence",
		Room:    c.room,
		From:    c.name,
		Text:    "left",
		At:      time.Now().UnixMilli(),
		Members: members,
	}, c)
}

func (h *Hub) switchRoom(c *Client, room, token string) {
	room = strings.TrimSpace(room)
	if room == "" || room == c.room {
		return
	}
	if len(room) > 48 {
		room = room[:48]
	}
	h.mu.RLock()
	need := h.tokens[room]
	members := len(h.rooms[room])
	h.mu.RUnlock()
	if need != "" && token != need {
		b, _ := json.Marshal(Message{Type: "error", Text: "invalid room token", At: time.Now().UnixMilli()})
		select {
		case c.send <- b:
		default:
		}
		return
	}
	if members >= h.maxRoom {
		b, _ := json.Marshal(Message{Type: "error", Text: "room full", At: time.Now().UnixMilli()})
		select {
		case c.send <- b:
		default:
		}
		return
	}

	old := c.room
	h.mu.Lock()
	if set := h.rooms[old]; set != nil {
		delete(set, c)
		if len(set) == 0 {
			delete(h.rooms, old)
		}
	}
	leftMembers := len(h.rooms[old])
	if h.rooms[room] == nil {
		h.rooms[room] = map[*Client]bool{}
	}
	h.rooms[room][c] = true
	c.room = room
	hist := append([]Message{}, h.history[room]...)
	joinedMembers := len(h.rooms[room])
	h.mu.Unlock()

	h.broadcast(old, Message{Type: "presence", Room: old, From: c.name, Text: "left", At: time.Now().UnixMilli(), Members: leftMembers}, c)
	for _, m := range hist {
		b, _ := json.Marshal(m)
		select {
		case c.send <- b:
		default:
		}
	}
	h.broadcast(room, Message{Type: "presence", Room: room, From: c.name, Text: "joined", At: time.Now().UnixMilli(), Members: joinedMembers}, nil)
}

func (h *Hub) broadcast(room string, msg Message, skip *Client) {
	if msg.ID == "" {
		msg.ID = shortID()
	}
	b, _ := json.Marshal(msg)
	if msg.Type == "chat" {
		h.mu.Lock()
		h.history[room] = append(h.history[room], msg)
		if len(h.history[room]) > h.histLimit {
			h.history[room] = h.history[room][len(h.history[room])-h.histLimit:]
		}
		h.mu.Unlock()
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.rooms[room] {
		if c == skip {
			continue
		}
		select {
		case c.send <- b:
		default:
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.leave(c)
		_ = c.conn.Close()
	}()
	_ = c.conn.SetReadDeadline(time.Now().Add(75 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		return nil
	})
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var in struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			Room  string `json:"room"`
			Token string `json:"token"`
		}
		if err := json.Unmarshal(data, &in); err != nil {
			continue
		}
		switch in.Type {
		case "chat":
			if in.Text == "" {
				continue
			}
			if len(in.Text) > 2000 {
				in.Text = in.Text[:2000]
			}
			if time.Since(c.lastChat) < c.hub.chatGap {
				continue
			}
			c.lastChat = time.Now()
			c.hub.broadcast(c.room, Message{
				Type: "chat",
				Room: c.room,
				From: c.name,
				Text: in.Text,
				At:   time.Now().UnixMilli(),
			}, nil)
		case "typing":
			c.hub.broadcast(c.room, Message{
				Type: "typing",
				Room: c.room,
				From: c.name,
				At:   time.Now().UnixMilli(),
			}, c)
		case "join":
			c.hub.switchRoom(c, in.Room, in.Token)
		case "ping":
			b, _ := json.Marshal(Message{Type: "pong", At: time.Now().UnixMilli()})
			select {
			case c.send <- b:
			default:
			}
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(25 * time.Second)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func shortID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().Format("150405.000000")
	}
	return hex.EncodeToString(b[:])
}
