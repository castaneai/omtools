package omtools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"open-match.dev/open-match/pkg/pb"
)

func TestDumpAssignment(t *testing.T) {
	as := &pb.Assignment{
		Connection: "test-connection",
		Extensions: map[string]*anypb.Any{},
	}
	val1, err := anypb.New(wrapperspb.String("str-value"))
	require.NoError(t, err)
	as.Extensions["key1"] = val1

	assert.Equal(t,
		`{"connection":"test-connection", "extensions":{"key1":{"@type":"type.googleapis.com/google.protobuf.StringValue", "value":"str-value"}}}`,
		DumpAssignment(as))
}
