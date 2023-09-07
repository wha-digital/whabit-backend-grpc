package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime/debug"

	"github.com/getsentry/sentry-go"
	middlewareRecovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func grpcCaptureRecover(serviceName string, sentryDSN string) middlewareRecovery.RecoveryHandlerFunc {
	return func(p any) (err error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
			}
		}()
		if sentryDSN != "" {
			event := &sentry.Event{
				Level:       sentry.LevelError,
				Environment: os.Getenv("SENTRY_ENVIRONMENT"),
				Exception:   []sentry.Exception{},
				Tags: map[string]string{
					"service_name": serviceName,
				},
			}
			switch reflect.TypeOf(p).Kind() {
			case reflect.String:
				mainTitle := fmt.Sprintf("[GRPC] %s", p.(string))
				event.Exception = append(event.Exception, sentry.Exception{
					Type:       mainTitle,
					Value:      serviceName,
					Stacktrace: sentry.NewStacktrace(),
				})
			default:
				if v, ok := p.(error); ok {
					mainTitle := fmt.Sprintf("[GRPC] %s", v.Error())
					event.Exception = append(event.Exception, sentry.Exception{
						Type:       mainTitle,
						Value:      serviceName,
						Stacktrace: sentry.NewStacktrace(),
					})
				}
			}
			sentry.CurrentHub().CaptureEvent(event)
		}

		log.Println("msg", "recovered from panic", p, "stack", string(debug.Stack()))
		return status.Errorf(codes.Internal, "%s", p)
	}
}

func UnaryServerGrpcCaptureException(serviceName string, sentryDSN string) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
			}
		}()

		resp, err = handler(ctx, req)
		status, statusOK := status.FromError(err)
		if statusOK && status != nil {
			if status.Code() == codes.Unknown &&
				status.Code() == codes.OK &&
				status.Code() == codes.NotFound {
				fmt.Printf("[GRPC-%s](%s) %s: %s \n", serviceName, info.FullMethod, status.Code(), status.Message())
				return resp, err
			}

			if sentryDSN != "" {
				sentry.WithScope(func(scope *sentry.Scope) {
					var reqBody = make([]byte, 0)
					if v, ok := req.(protoreflect.ProtoMessage); ok {
						reqBody, _ = protojson.Marshal(v)
					} else {
						reqBody, _ = json.Marshal(req)
					}

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
						Extra: map[string]interface{}{
							"body": string(reqBody),
						},
						Exception: []sentry.Exception{
							{
								Type:  mainTitle,
								Value: subTitle,
							},
						},
					}
					sentry.CurrentHub().CaptureEvent(event)
				})
			}
		}
		return resp, err
	}
}
