package main

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kianPk/pulsehub/internal/hub"
)

//go:embed web/*
var webFS embed.FS

func main() {
	addr := env("PULSE_LISTEN", ":8090")
	logger := log.New(os.Stdout, "pulsehub ", log.LstdFlags|log.Lmsgprefix)
	h := hub.New(logger)

	// Optional locked rooms: PULSE_ROOM_TOKENS=private:secret,ops:ops-token
	if raw := os.Getenv("PULSE_ROOM_TOKENS"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
			if len(kv) == 2 {
				h.SetRoomToken(kv[0], kv[1])
			}
		}
	}

	webRoot, err := fs.Sub(webFS, "web")
	if err != nil {
		logger.Fatal(err)
	}
	static := http.FileServer(http.FS(webRoot))

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "service": "pulsehub"})
	})
	mux.HandleFunc("/v1/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.Stats())
	})
	mux.HandleFunc("/v1/rooms", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rooms": h.Rooms()})
	})
	mux.HandleFunc("/v1/presence", func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		if room == "" {
			room = "lobby"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"room": room, "members": h.Presence(room)})
	})
	mux.HandleFunc("/ws", h.ServeWS)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || strings.HasSuffix(r.URL.Path, ".html") ||
			strings.HasSuffix(r.URL.Path, ".js") || strings.HasSuffix(r.URL.Path, ".css") {
			static.ServeHTTP(w, r)
			return
		}
		static.ServeHTTP(w, r)
	}))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Printf("listening on %s · websocket hub · rooms+presence", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
