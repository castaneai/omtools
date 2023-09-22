package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"open-match.dev/open-match/pkg/pb"
)

func main() {
	if len(os.Args) != 3 {
		log.Printf("Usage: om-ticket-cleaner <REDIS_ADDR> <STALE_TIME>")
		log.Printf("Example: om-ticket-cleaner 127.0.0.1:6379 10m")
		os.Exit(2)
	}
	addr := os.Args[1]
	staleTime, err := time.ParseDuration(os.Args[2])
	if err != nil {
		log.Printf("failed to parse duration: %s: %+v", os.Args[2], err)
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if _, err := client.Ping(ctx).Result(); err != nil {
		log.Fatalf("failed to connect to Redis: %+v", err)
	}
	result, err := client.SMembers(ctx, "allTickets").Result()
	if err != nil {
		log.Fatalf("failed to SMEMBERS: %+v", err)
	}

	var keysToDelete []string
	for _, ticketKey := range result {
		data, err := client.Get(ctx, ticketKey).Result()
		if err != nil {
			log.Printf("failed to GET ticket: %s: %+v", ticketKey, err)
			continue
		}
		ticket, err := decodeTicket(data)
		if err != nil {
			log.Printf("failed to decode ticket: %+v", err)
			continue
		}
		if time.Since(ticket.CreateTime.AsTime()) >= staleTime {
			log.Printf("delete '%s' (created at %s)", ticket.Id, ticket.CreateTime.AsTime())
			keysToDelete = append(keysToDelete, ticketKey)
		}
	}
	if len(keysToDelete) > 0 {
		if _, err := client.SRem(ctx, "allTickets", keysToDelete).Result(); err != nil {
			log.Fatalf("failed to remove ticket keys from allTickets: %+v", err)
		}
		if _, err := client.Del(ctx, keysToDelete...).Result(); err != nil {
			log.Fatalf("failed to delete ticket keys: %+v", err)
		}
	}
}

func decodeTicket(data string) (*pb.Ticket, error) {
	var t pb.Ticket
	b := []byte(data)
	// HACK: miniredis support
	if decoded, err := base64.StdEncoding.DecodeString(data); err == nil {
		b = decoded
	}
	if err := proto.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Ticket: %w", err)
	}
	return &t, nil
}
