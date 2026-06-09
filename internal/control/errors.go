package control

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")

const writeIndeterminateMessage = "cache write indeterminate"

func WriteIndeterminateError(err error) error {
	if err == nil {
		return status.Error(codes.Aborted, writeIndeterminateMessage)
	}
	return status.Errorf(codes.Aborted, "%s: %v", writeIndeterminateMessage, err)
}

func IsWriteIndeterminate(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Aborted && strings.HasPrefix(s.Message(), writeIndeterminateMessage)
}
