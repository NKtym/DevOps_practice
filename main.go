package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountNSPath    = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	serviceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	defaultPort             = "8080"
)

type HealthResp struct {
	Status string    `json:"status"`
	Time   time.Time `json:"time"`
}

func writeJSON(w http.ResponseWriter, v any, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func normalizeExternalURL(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, "/")

	if raw == "" {
		return ""
	}

	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}

	return raw
}

func fetchExternalHealth(ctx context.Context) (map[string]any, error) {
	externalURL := normalizeExternalURL(os.Getenv("EXTERNAL_APP_URL"))
	if externalURL == "" {
		return map[string]any{
			"status": "error",
			"error":  "EXTERNAL_APP_URL not configured",
		}, fmt.Errorf("EXTERNAL_APP_URL not configured")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, externalURL+"/health", nil)
	if err != nil {
		return map[string]any{
			"status": "error",
			"url":    externalURL + "/health",
			"error":  err.Error(),
		}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return map[string]any{
			"status": "error",
			"url":    externalURL + "/health",
			"error":  err.Error(),
		}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = string(body)
	}

	if resp.StatusCode != http.StatusOK {
		return map[string]any{
			"status":       "error",
			"url":          externalURL + "/health",
			"partner_code": resp.StatusCode,
			"partner_body": payload,
		}, nil
	}

	return map[string]any{
		"status":       "success",
		"url":          externalURL + "/health",
		"partner_code": resp.StatusCode,
		"data":         payload,
	}, nil
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

func getRunningPodsCount(ctx context.Context) (int, error) {
	nsBytes, err := os.ReadFile(serviceAccountNSPath)
	if err != nil {
		return 0, fmt.Errorf("read namespace: %w", err)
	}
	namespace := strings.TrimSpace(string(nsBytes))
	if namespace == "" {
		return 0, fmt.Errorf("namespace is empty")
	}

	tokenBytes, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return 0, fmt.Errorf("read token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return 0, fmt.Errorf("token is empty")
	}

	caBytes, err := os.ReadFile(serviceAccountCAPath)
	if err != nil {
		return 0, fmt.Errorf("read CA: %w", err)
	}

	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(caBytes); !ok {
		return 0, fmt.Errorf("append CA cert failed")
	}

	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if host == "" {
		return 0, fmt.Errorf("KUBERNETES_SERVICE_HOST is empty")
	}

	url := fmt.Sprintf("https://%s:%s/api/v1/namespaces/%s/pods", host, port, namespace)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: roots,
			},
		},
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("call kubernetes api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("kubernetes api status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var podList struct {
		Items []struct {
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&podList); err != nil {
		return 0, fmt.Errorf("decode pods list: %w", err)
	}

	running := 0
	for _, pod := range podList.Items {
		if pod.Status.Phase == "Running" {
			running++
		}
	}

	return running, nil
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, HealthResp{
			Status: "ok",
			Time:   time.Now().UTC(),
		}, http.StatusOK)
	})

	mux.HandleFunc("/partner", func(w http.ResponseWriter, r *http.Request) {
		data, err := fetchExternalHealth(r.Context())
		if err != nil {
			writeJSON(w, data, http.StatusBadGateway)
			return
		}
		writeJSON(w, map[string]any{
			"message":      "Успешное взаимодействие между приложениями разных групп",
			"my_namespace": os.Getenv("POD_NAMESPACE"),
			"external_app": data,
		}, http.StatusOK)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		runningPods, err := getRunningPodsCount(r.Context())
		if err != nil {
			writeJSON(w, map[string]string{"error": err.Error()}, http.StatusServiceUnavailable)
			return
		}

		externalData, extErr := fetchExternalHealth(r.Context())
		statusCode := http.StatusOK
		if extErr != nil {
			statusCode = http.StatusBadGateway
		}

		writeJSON(w, map[string]any{
			"message": "Hello from containerized app!",
			"status":  "running",
			"pod_count": map[string]any{
				"count":      runningPods,
				"namespace":  os.Getenv("POD_NAMESPACE"),
				"total_pods": runningPods,
			},
			"external_app_data": externalData,
			"namespace":         os.Getenv("POD_NAMESPACE"),
		}, statusCode)
	})

	srv := &http.Server{
		Addr:         ":" + defaultPort,
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
