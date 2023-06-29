package omtools

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"open-match.dev/open-match/pkg/pb"
)

type mockBackend struct {
	fetchCounter int
}

func (b *mockBackend) FetchMatches(req *pb.FetchMatchesRequest, stream pb.BackendService_FetchMatchesServer) error {
	b.fetchCounter++
	if b.fetchCounter >= 2 {
		if err := stream.Send(&pb.FetchMatchesResponse{Match: &pb.Match{MatchId: "test"}}); err != nil {
			return err
		}
		return nil
	}
	return status.Errorf(codes.Unavailable, "temporary error: %d", b.fetchCounter)
}

func (b *mockBackend) AssignTickets(ctx context.Context, req *pb.AssignTicketsRequest) (*pb.AssignTicketsResponse, error) {
	return &pb.AssignTicketsResponse{}, nil
}

func (b *mockBackend) ReleaseTickets(ctx context.Context, req *pb.ReleaseTicketsRequest) (*pb.ReleaseTicketsResponse, error) {
	return &pb.ReleaseTicketsResponse{}, nil
}

func (b *mockBackend) ReleaseAllTickets(ctx context.Context, req *pb.ReleaseAllTicketsRequest) (*pb.ReleaseAllTicketsResponse, error) {
	return &pb.ReleaseAllTicketsResponse{}, nil
}

func setupMockBackend(t *testing.T) pb.BackendServiceClient {
	lis := bufconn.Listen(1024)
	t.Cleanup(func() { lis.Close() })
	s := grpc.NewServer()
	t.Cleanup(func() { s.Stop() })
	pb.RegisterBackendServiceServer(s, &mockBackend{})
	go func() { s.Serve(lis) }()
	ctx := context.Background()
	cc, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial via bufconn: %+v", err)
	}
	return pb.NewBackendServiceClient(cc)
}

type dummyAssigner struct{}

func (a *dummyAssigner) Assign(ctx context.Context, matches []*pb.Match) ([]*pb.AssignmentGroup, error) {
	return nil, nil
}

func TestDirector(t *testing.T) {
	profile := &pb.MatchProfile{
		Name:  "test-profile",
		Pools: []*pb.Pool{},
	}
	mfConfig := &pb.FunctionConfig{}
	backend := setupMockBackend(t)
	director := NewDirector(backend, profile, mfConfig, &dummyAssigner{})
	ctx := context.Background()
	assert.NoError(t, director.Run(ctx, 1*time.Second))
}
