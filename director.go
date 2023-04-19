package omtools

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/sethvargo/go-retry"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"open-match.dev/open-match/pkg/pb"
)

const (
	maxRetries                  = 10
	retryBackoffInitialDuration = 200 * time.Millisecond
	retryBackoffMaxDuration     = 10 * time.Second
	retryJitterPercentage       = 5
)

func newRetryBackoff() retry.Backoff {
	r := retry.NewExponential(retryBackoffInitialDuration)
	r = retry.WithJitterPercent(retryJitterPercentage, r) // 5% +/- jitter
	r = retry.WithCappedDuration(retryBackoffMaxDuration, r)
	r = retry.WithMaxRetries(maxRetries, r)
	return r
}

type Assigner interface {
	Assign(ctx context.Context, matches []*pb.Match) ([]*pb.AssignmentGroup, error)
}

type Director struct {
	backend  pb.BackendServiceClient
	profile  *pb.MatchProfile
	mfConfig *pb.FunctionConfig
	assigner Assigner
	logger   *slog.Logger
}

func NewDirector(backend pb.BackendServiceClient,
	profile *pb.MatchProfile, mfConfig *pb.FunctionConfig,
	assigner Assigner) *Director {
	return &Director{
		backend:  backend,
		profile:  profile,
		mfConfig: mfConfig,
		assigner: assigner,
		logger:   slog.Default(), // TODO(castaneai): custom logger
	}
}

func isRetryableError(err error) bool {
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.Unavailable {
		return true
	}
	return false
}

func (d *Director) Run(ctx context.Context, period time.Duration) error {
	d.logger.Debug(fmt.Sprintf("director started (period: %s)", period))
	b := newRetryBackoff()
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var matches []*pb.Match
			if err := retry.Do(ctx, b, func(ctx context.Context) error {
				ms, err := d.FetchMatches(ctx)
				if err != nil {
					d.logger.Error(fmt.Sprintf("failed to fetch matches: %+v", err))
					if isRetryableError(err) {
						return retry.RetryableError(err)
					}
					return err
				}
				matches = append(matches, ms...)
				return nil
			}); err != nil {
				return err
			}

			if len(matches) > 0 {
				if err := retry.Do(ctx, b, func(ctx context.Context) error {
					if _, err := d.AssignTickets(ctx, matches); err != nil {
						if isRetryableError(err) {
							return retry.RetryableError(err)
						}
						return err
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}
	}
}

func (d *Director) FetchMatches(ctx context.Context) ([]*pb.Match, error) {
	stream, err := d.backend.FetchMatches(ctx, &pb.FetchMatchesRequest{
		Config:  d.mfConfig,
		Profile: d.profile,
	})
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
