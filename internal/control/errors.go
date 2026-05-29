package control

import "google.golang.org/grpc/status"
import "google.golang.org/grpc/codes"

var ErrUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")
