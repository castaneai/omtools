package omtools

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type HasExtensions interface {
	GetExtensions() map[string]*anypb.Any
}

func getExt[T proto.Message](obj HasExtensions, key string, ptr T) (bool, error) {
	if obj == nil {
		return false, fmt.Errorf("object which has Extensions is nil")
	}
	exts := obj.GetExtensions()
	if exts == nil {
		return false, nil
	}
	val, ok := exts[key]
	if !ok {
		return false, nil
	}
	if err := val.UnmarshalTo(ptr); err != nil {
		return true, fmt.Errorf("failed to unmarshal Any to string value: %w", err)
	}
	return true, nil
}

func getStrExt(obj HasExtensions, key string) string {
	var val wrapperspb.StringValue
	ok, err := getExt(obj, key, &val)
	if err != nil || !ok {
		return ""
	}
	return val.Value
}

func getIntExt(obj HasExtensions, key string) int {
	var val wrapperspb.Int32Value
	ok, err := getExt(obj, key, &val)
	if err != nil || !ok {
		return 0
	}
	return int(val.Value)
}
