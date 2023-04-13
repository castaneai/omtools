package omtools

import (
	"context"
	"fmt"
	"io"

	"open-match.dev/open-match/pkg/pb"
)

type Assigner interface {
	Assign(ctx context.Context, matches []*pb.Match) ([]*pb.AssignmentGroup, error)
}

type Director struct {
	backend  pb.BackendServiceClient
	assigner Assigner
}

func NewDirector(backend pb.BackendServiceClient, assigner Assigner) *Director {
	return &Director{
		backend:  backend,
		assigner: assigner,
	}
}

func (d *Director) FetchMatches(ctx context.Context, profile *pb.MatchProfile, mfConfig *pb.FunctionConfig) ([]*pb.Match, error) {
	stream, err := d.backend.FetchMatches(ctx, &pb.FetchMatchesRequest{Config: mfConfig, Profile: profile})
	if err != nil {
		return nil, err
	}
	var matches []*pb.Match
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to recv matches: %w", err)
		}
		matches = append(matches, resp.Match)
	}
	return matches, nil
}

func (d *Director) AssignTickets(ctx context.Context, matches []*pb.Match) ([]*pb.AssignmentGroup, error) {
	asgs, err := d.assigner.Assign(ctx, matches)
	if err != nil {
		return nil, fmt.Errorf("failed to assign tickets: %w", err)
	}
	if _, err := d.backend.AssignTickets(ctx, &pb.AssignTicketsRequest{Assignments: asgs}); err != nil {
		return nil, fmt.Errorf("failed to assign tickets: %w", err)
	}
	return asgs, nil
}
