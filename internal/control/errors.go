package control

import (
	"errors"
	"strings"

	"github.com/infamousity/distributed-cache/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")

const writeIndeterminateMessage = "cache write indeterminate"

type VersionConflictError struct {
	Current version.Version
	err     error
}

func (e *VersionConflictError) Error() string {
	if e == nil || e.err == nil {
		return "stale cache version"
	}
	return e.err.Error()
}

func (e *VersionConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *VersionConflictError) GRPCStatus() *status.Status {
	if e == nil || e.err == nil {
		return status.New(codes.Aborted, "stale cache version")
	}
	return status.Convert(e.err)
}

func (e *VersionConflictError) CurrentVersion() version.Version {
	if e == nil {
		return version.Zero()
	}
	return e.Current
}

func ConflictVersion(err error) (version.Version, bool) {
	var conflict *VersionConflictError
	if !errors.As(err, &conflict) || conflict == nil || conflict.Current.IsZero() {
		return version.Zero(), false
	}
	return conflict.Current, true
}

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
