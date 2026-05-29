package control

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const authHeader = "x-cache-token"

func UnaryServerAuth(sharedKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if sharedKey != "" {
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return nil, ErrUnauthenticated
			}
			vals := md.Get(authHeader)
			if len(vals) == 0 || subtle.ConstantTimeCompare([]byte(vals[0]), []byte(sharedKey)) != 1 {
				return nil, ErrUnauthenticated
			}
		}
		return handler(ctx, req)
	}
}

func UnaryClientAuth(sharedKey string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if sharedKey != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, authHeader, sharedKey)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
