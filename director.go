package omtools

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/sethvargo/go-retry"
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
	logger   Logger
}

type directorOptions struct {
	Logger Logger
}

func defaultDirectorOptions() *directorOptions {
	return &directorOptions{
		Logger: defaultLogger,
	}
}

type DirectorOption interface {
	apply(options *directorOptions)
}

type DirectorOptionFunc func(options *directorOptions)

func (f DirectorOptionFunc) apply(options *directorOptions) {
	f(options)
}

func NewDirector(backend pb.BackendServiceClient,
	profile *pb.MatchProfile, mfConfig *pb.FunctionConfig,
	assigner Assigner, opts ...DirectorOption) *Director {
	dopts := defaultDirectorOptions()
	for _, opt := range opts {
		opt.apply(dopts)
	}
	return &Director{
		backend:  backend,
		profile:  profile,
		mfConfig: mfConfig,
		assigner: assigner,
		logger:   dopts.Logger,
	}
}

func WithDirectorLogger(logger Logger) DirectorOption {
	return DirectorOptionFunc(func(options *directorOptions) {
		options.Logger = logger
	})
}

func isRetryableError(err error) bool {
	// Open Match consists of many components,
	// and when some components are temporarily down, it usually returns code: Unavailable.
	if st := new(status.Status); asStatus(err, &st) {
		return st.Code() == codes.Unavailable || containsUnavailableError(st)
	}
	return false
}

func containsUnavailableError(st *status.Status) bool {
	// If Match Function is temporarily down,
	// you get an Unavailable error wrapped with code = Unknown.
	return strings.Contains(st.String(), "rpc error: code = Unavailable")
}

func (d *Director) Run(ctx context.Context, tickRate time.Duration) error {
	d.logger.Infof("director started (tickRate: %s)", tickRate)
	b := newRetryBackoff()
	ticker := time.NewTicker(tickRate)
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
					if isRetryableError(err) {
						d.logger.Debugf("failed to fetch matches, retrying...: %+v", err)
						return retry.RetryableError(err)
					}
					d.logger.Errorf("failed to fetch matches: %+v", err)
					return err
				}
				matches = append(matches, ms...)
				return nil
			}); err != nil {
				return err
			}

			if len(matches) > 0 {
				if _, err := d.AssignTickets(ctx, matches); err != nil {
					if isRetryableError(err) {
						return retry.RetryableError(err)
					}
					return err
				}
				return nil
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
