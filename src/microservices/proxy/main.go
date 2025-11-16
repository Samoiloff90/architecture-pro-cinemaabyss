package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

var (
	monolithURL            string
	moviesServiceURL       string
	eventsServiceURL       string
	gradualMigration       bool
	moviesMigrationPercent int
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	// Load configuration from environment
	port := getEnv("PORT", "8000")
	monolithURL = getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	eventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")

	gradualMigration = getEnv("GRADUAL_MIGRATION", "true") == "true"
	moviesMigrationPercent = getEnvAsInt("MOVIES_MIGRATION_PERCENT", 50)

	log.Printf("Starting Proxy Service on port %s", port)
	log.Printf("Monolith URL: %s", monolithURL)
	log.Printf("Movies Service URL: %s", moviesServiceURL)
	log.Printf("Events Service URL: %s", eventsServiceURL)
	log.Printf("Gradual Migration: %v", gradualMigration)
	log.Printf("Movies Migration Percent: %d%%", moviesMigrationPercent)

	// Health check endpoint
	http.HandleFunc("/health", healthHandler)

	// Proxy endpoints
	http.HandleFunc("/api/movies", moviesProxyHandler)
	http.HandleFunc("/api/movies/health", moviesHealthProxyHandler)
	http.HandleFunc("/api/users", usersProxyHandler)
	http.HandleFunc("/api/payments", paymentsProxyHandler)
	http.HandleFunc("/api/subscriptions", subscriptionsProxyHandler)
	http.HandleFunc("/api/events", eventsProxyHandler)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": true, "service": "proxy"}`)
}

// moviesProxyHandler - Strangler Fig Pattern implementation
func moviesProxyHandler(w http.ResponseWriter, r *http.Request) {
	var targetURL string

	// Decide where to route based on migration percentage
	if gradualMigration && shouldMigrateRequest(moviesMigrationPercent) {
		targetURL = moviesServiceURL
		log.Printf("[Strangler Fig] Routing /api/movies to Movies Service (%d%%)", moviesMigrationPercent)
	} else {
		targetURL = monolithURL
		log.Printf("[Strangler Fig] Routing /api/movies to Monolith (%d%%)", 100-moviesMigrationPercent)
	}

	proxyRequest(w, r, targetURL, "/api/movies")
}

func moviesHealthProxyHandler(w http.ResponseWriter, r *http.Request) {
	// Always route health checks to the microservice
	proxyRequest(w, r, moviesServiceURL, "/api/movies/health")
}

func usersProxyHandler(w http.ResponseWriter, r *http.Request) {
	// Users are still in monolith
	proxyRequest(w, r, monolithURL, "/api/users")
}

func paymentsProxyHandler(w http.ResponseWriter, r *http.Request) {
	// Payments are still in monolith
	proxyRequest(w, r, monolithURL, "/api/payments")
}

func subscriptionsProxyHandler(w http.ResponseWriter, r *http.Request) {
	// Subscriptions are still in monolith
	proxyRequest(w, r, monolithURL, "/api/subscriptions")
}

func eventsProxyHandler(w http.ResponseWriter, r *http.Request) {
	// Route to events service
	proxyRequest(w, r, eventsServiceURL, "/api/events")
}

// proxyRequest forwards the request to the target service
func proxyRequest(w http.ResponseWriter, r *http.Request, targetURL string, path string) {
	target, err := url.Parse(targetURL)
	if err != nil {
		log.Printf("Error parsing target URL: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Modify the request
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.URL.Path = path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = target.Host
	}

	// Handle errors
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error: %v", err)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
	}

	proxy.ServeHTTP(w, r)
}

// shouldMigrateRequest determines if request should go to new service
// based on migration percentage (0-100)
func shouldMigrateRequest(percentage int) bool {
	if percentage <= 0 {
		return false // 0% - all traffic to monolith
	}
	if percentage >= 100 {
		return true // 100% - all traffic to microservice
	}
	// Random decision based on percentage
	return rand.Intn(100) < percentage
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}
