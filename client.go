package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
Example Default
  - NewClient(JAEGER_SERVICE_NAME, SENTRY_DSN)

Example with Addon ServerOption
  - options = grpc.MaxRecvMsgSize(grpcMaxReceiveSize) = ขยายขนาด Body buffer ที่รับได้
  - NewClient(JAEGER_SERVICE_NAME, SENTRY_DSN, options)
*/
func NewServer(serviceName string, sentryDSN string, opt ...grpc.ServerOption) *grpc.Server {
	tracer := opentracing.GlobalTracer()

	options := make([]grpc.ServerOption, 0)
	options = append(options, opt...)
	options = append(options,
		grpc.ChainUnaryInterceptor(
			grpc.UnaryServerInterceptor(
				otgrpc.OpenTracingServerInterceptor(tracer),
			),
			recovery.UnaryServerInterceptor(
				recovery.WithRecoveryHandler(grpcCaptureRecover(serviceName, sentryDSN)),
			),
			grpc.UnaryServerInterceptor(
				UnaryServerGrpcCaptureException(serviceName, sentryDSN),
			),
		),
		grpc.ChainStreamInterceptor(
			grpc.StreamServerInterceptor(
				otgrpc.OpenTracingStreamServerInterceptor(tracer),
			),
			recovery.StreamServerInterceptor(
				recovery.WithRecoveryHandler(grpcCaptureRecover(serviceName, sentryDSN)),
			),
			grpc.StreamServerInterceptor(
				StreamServerGrpcCaptureException(serviceName, sentryDSN),
			),
		),
	)

	return grpc.NewServer(options...)
}

/*
Example
  - NewClient(context.Background(), "localhost:3100", 30)
*/
func NewClient(ctx context.Context, grpcAddress string, timeoutSecond int, opt ...grpc.DialOption) (context.Context, context.CancelFunc, *grpc.ClientConn, error) {
	options := make([]grpc.DialOption, 0)
	options = append(options, opt...)
	options = append(options,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		),
		grpc.WithStreamInterceptor(
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
		),
	)

	conn, err := grpc.DialContext(ctx, grpcAddress, options...)
	if err != nil {
		return ctx, nil, nil, fmt.Errorf("fail to connect on service with address %s", grpcAddress)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSecond*int(time.Second)))
	return ctx, cancel, conn, nil
}
