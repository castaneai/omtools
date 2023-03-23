package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"open-match.dev/open-match/pkg/pb"
)

func main() {
	if len(os.Args) != 3 {
		log.Printf("Usage: ./om-ticket-cleaner <REDIS_ADDR> <STALE_TIME>")
		log.Printf("Example: ./om-ticket-cleaner 127.0.0.1:6379 10m")
		os.Exit(2)
	}
	addr := os.Args[1]
	staleTime, err := time.ParseDuration(os.Args[2])
	if err != nil {
		log.Printf("failed to parse duration: %s: %+v", os.Args[2], err)
		os.Exit(2)
	}
	log.Printf("cleaning staled tickets...(redis: %s, stale time: %s)", addr, staleTime)

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	ctx := context.Background()
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
		var ticket pb.Ticket
		if err := proto.Unmarshal([]byte(data), &ticket); err != nil {
			log.Printf("failed to unmarshal ticket data: %+v", err)
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
