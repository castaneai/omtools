package omtools

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"open-match.dev/open-match/pkg/pb"
)

func DumpAssignment(as *pb.Assignment) string {
	b, err := protojson.Marshal(as)
	if err != nil {
		return fmt.Sprintf("<!MARSHAL_ERROR: %+v>", err)
	}
	return string(b)
}
