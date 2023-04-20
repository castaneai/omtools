package omtools

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// see also https://github.com/grpc/grpc-go/issues/2934
func asStatus(err error, target **status.Status) bool {
	cerr := err
	for {
		st, ok := status.FromError(cerr)
		if ok {
			*target = st
			return true
		}
		nerr := errors.Unwrap(cerr)
		if nerr == nil {
			return false
		}
		cerr = nerr
	}
}

func hasStatusCode(err error, code codes.Code) bool {
	if status := new(status.Status); asStatus(err, &status) {
		return status.Code() == code
	}
	return false
}
