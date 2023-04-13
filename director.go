package omtools

import (
	"context"
	"fmt"
	"io"
	"time"

	"open-match.dev/open-match/pkg/pb"
)

type Assigner interface {
	Assign(ctx context.Context, matches []*pb.Match) ([]*pb.AssignmentGroup, error)
}

type Director struct {
	backend  pb.BackendServiceClient
	profile  *pb.MatchProfile
	mfConfig *pb.FunctionConfig
	assigner Assigner
	// TODO(castaneai): logger
}

func NewDirector(backend pb.BackendServiceClient, profile *pb.MatchProfile, mfConfig *pb.FunctionConfig, assigner Assigner) *Director {
	return &Director{
		backend:  backend,
		profile:  profile,
		mfConfig: mfConfig,
		assigner: assigner,
	}
}

func (d *Director) Run(ctx context.Context, period time.Duration) error {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			matches, err := d.FetchMatches(ctx, d.profile, d.mfConfig)
			if err != nil {
				return err
			}
			if _, err := d.AssignTickets(ctx, matches); err != nil {
				return err
			}
		}
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
