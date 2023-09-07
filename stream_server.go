package grpc

import (
	"fmt"
	"log"
	"os"

	"github.com/getsentry/sentry-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func StreamServerGrpcCaptureException(serviceName string, sentryDSN string) func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
			}
		}()

		err := handler(srv, stream)
		status, statusOK := status.FromError(err)
		if statusOK && status != nil {
			if status.Code() == codes.Unknown &&
				status.Code() == codes.OK &&
				status.Code() == codes.NotFound {
				fmt.Printf("[GRPC-%s](%s) %s: %s \n", serviceName, info.FullMethod, status.Code(), status.Message())
				return err
			}

			if sentryDSN != "" {
				mainTitle := fmt.Sprintf("[GRPC] %s: %s", status.Code(), status.Message())
				subTitle := fmt.Sprintf("%s: %s", serviceName, info.FullMethod)
				log.Println(mainTitle)
				log.Println(subTitle)

				event := &sentry.Event{
					Level:       sentry.LevelError,
					Environment: os.Getenv("SENTRY_ENVIRONMENT"),
					Tags: map[string]string{
						"service_name": serviceName,
					},
					Exception: []sentry.Exception{
						{
							Type:  mainTitle,
							Value: subTitle,
						},
					},
				}
				sentry.CurrentHub().CaptureEvent(event)
			}
		}
		return err
	}
}
