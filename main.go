package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type RootResp struct {
	Message   string    `json:"message"`
	StartedAt time.Time `json:"started_at"`
}

type HealthResp struct {
	Status string    `json:"status"`
	Time   time.Time `json:"time"`
}

func writeJSON(w http.ResponseWriter, v interface{}, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func performCleanup(ctx context.Context) {
	log.Println("Background worker: cleanup start")
	select {
	case <-ctx.Done():
		log.Println("Background worker: cleanup timeout")
	default:
		log.Println("Background worker: cleanup complete")
	}
}

func backgroundWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	log.Println("Background worker: started")
	for {
		select {
		case <-ctx.Done():
			log.Println("Background worker: received shutdown signal, cleaning up")
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			performCleanup(cleanupCtx)
			cancel()
			return
		case t := <-ticker.C:
			log.Printf("Background worker: tick at %s\n", t.UTC().Format(time.RFC3339))
		}
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func main() {
	startedAt := time.Now().UTC()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, RootResp{
			Message:   "Good",
			StartedAt: startedAt,
		}, http.StatusOK)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, HealthResp{
			Status: "ok",
			Time:   time.Now().UTC(),
		}, http.StatusOK)
	})
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go backgroundWorker(ctx, &wg)
	go func() {
		log.Printf("Server starting on %s\n", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stop
	log.Printf("Signal received: %v", sig)
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v\n", err)
	} else {
		log.Println("HTTP server stopped gracefully")
	}
	wg.Wait()
	log.Println("Background tasks finished")
}
