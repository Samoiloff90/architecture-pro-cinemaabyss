package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaBrokers []string
	kafkaWriter  *kafka.Writer
)

// Event types
type Event struct {
	Type      string                 `json:"type"`   // user, movie, payment
	Action    string                 `json:"action"` // registered, viewed, success
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

func main() {
	port := getEnv("PORT", "8082")
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers = strings.Split(kafkaBrokersStr, ",")

	log.Printf("Starting Events Service on port %s", port)
	log.Printf("Kafka Brokers: %v", kafkaBrokers)

	// Initialize Kafka writer
	initKafkaWriter()
	defer kafkaWriter.Close()

	// Start Kafka consumer in background
	go startKafkaConsumer()

	// HTTP endpoints
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events", eventsHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initKafkaWriter() {
	kafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers...),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": true, "service": "events"}`)
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event.Timestamp = time.Now()

	// Publish to Kafka
	if err := publishEvent(event); err != nil {
		log.Printf("Error publishing event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Event published successfully",
		"event":   event,
	})
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		Type:      "user",
		Action:    getStringFromMap(data, "action", "registered"),
		Data:      data,
		Timestamp: time.Now(),
	}

	if err := publishEvent(event); err != nil {
		log.Printf("Error publishing user event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "User event published",
		"event":   event,
	})
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		Type:      "movie",
		Action:    getStringFromMap(data, "action", "viewed"),
		Data:      data,
		Timestamp: time.Now(),
	}

	if err := publishEvent(event); err != nil {
		log.Printf("Error publishing movie event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Movie event published",
		"event":   event,
	})
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		Type:      "payment",
		Action:    getStringFromMap(data, "action", "success"),
		Data:      data,
		Timestamp: time.Now(),
	}

	if err := publishEvent(event); err != nil {
		log.Printf("Error publishing payment event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Payment event published",
		"event":   event,
	})
}

func publishEvent(event Event) error {
	topic := fmt.Sprintf("%s-events", event.Type)

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Topic: topic,
		Key:   []byte(event.Type),
		Value: eventJSON,
		Time:  event.Timestamp,
	}

	ctx := context.Background()
	err = kafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		return err
	}

	log.Printf("[Producer] Published event to topic '%s': %s", topic, string(eventJSON))
	return nil
}

func startKafkaConsumer() {
	topics := []string{"user-events", "movie-events", "payment-events"}

	for _, topic := range topics {
		go consumeTopic(topic)
	}
}

func consumeTopic(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		Topic:    topic,
		GroupID:  "events-service-consumer",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	log.Printf("[Consumer] Starting consumer for topic: %s", topic)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[Consumer] Error reading message from %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[Consumer] Error unmarshaling event: %v", err)
			continue
		}

		log.Printf("[Consumer] Received event from topic '%s': type=%s, action=%s, data=%v",
			topic, event.Type, event.Action, event.Data)
	}
}

func getStringFromMap(m map[string]interface{}, key, defaultValue string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return defaultValue
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
