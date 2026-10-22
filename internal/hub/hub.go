package hub

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

type Message struct {
	Type    string `json:"type"`
	Room    string `json:"room"`
	From    string `json:"from"`
	Text    string `json:"text,omitempty"`
	At      int64  `json:"at"`
	Members int    `json:"members,omitempty"`
}

type Client struct {
	id   string
	name string
	room string
	conn *websocket.Conn
	send chan []byte
	hub  *Hub
}

type Hub struct {
	mu      sync.RWMutex
	rooms   map[string]map[*Client]bool
	history map[string][]Message
	log     *log.Logger
}

func New(logger *log.Logger) *Hub {
	return &Hub{
		rooms:   map[string]map[*Client]bool{},
		history: map[string][]Message{},
		log:     logger,
	}
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
	return map[string]any{"rooms": rooms, "clients": clients}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "anon"
	}
	room := r.URL.Query().Get("room")
	if room == "" {
		room = "lobby"
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
		send: make(chan []byte, 32),
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

func (h *Hub) broadcast(room string, msg Message, skip *Client) {
	b, _ := json.Marshal(msg)
	if msg.Type == "chat" {
		h.mu.Lock()
		h.history[room] = append(h.history[room], msg)
		if len(h.history[room]) > 100 {
			h.history[room] = h.history[room][len(h.history[room])-100:]
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
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var in struct {
			Type string `json:"type"`
			Text string `json:"text"`
			Room string `json:"room"`
		}
		if err := json.Unmarshal(data, &in); err != nil {
			continue
		}
		switch in.Type {
		case "chat":
			if in.Text == "" {
				continue
			}
			c.hub.broadcast(c.room, Message{
				Type: "chat",
				Room: c.room,
				From: c.name,
				Text: in.Text,
				At:   time.Now().UnixMilli(),
			}, nil)
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
	return time.Now().Format("150405.000000")
}
