package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/microsoft/durabletask-go/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func InterceptorLogger(l backend.Logger) logging.Logger {
	return logging.LoggerFunc(func(_ context.Context, lvl logging.Level, msg string, fields ...any) {
		var fieldsStr []string
		for i := 0; i < len(fields)/2; i++ {
			fieldsStr = append(fieldsStr, fmt.Sprintf("%v:%v", fields[2*i], fields[2*i+1]))
		}

		logline := fmt.Sprintf("<GPRC Interceptor Log> %v: %v", msg, strings.Join(fieldsStr, "\t"))
		switch lvl {
		case logging.LevelDebug:
			l.Debug(logline)
		case logging.LevelInfo:
			l.Info(logline)
		case logging.LevelWarn:
			l.Warn(logline)
		case logging.LevelError:
			l.Error(logline)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}

type azureGrpcCredentials struct {
	token               *azcore.AccessToken
	TokenOptions        policy.TokenRequestOptions
	logger              backend.Logger
	azureCredential     azcore.TokenCredential
	disableTokenRefresh bool
	insecure            bool
}

func newAzureGrpcCredentials(ctx context.Context, cred azcore.TokenCredential, scopes []string, tenantId string, logger backend.Logger, disableTokenRefresh bool, insecure bool) (azureGrpcCredentials, error) {
	tokenOptions := policy.TokenRequestOptions{
		Claims:    "",
		EnableCAE: false,
		Scopes:    []string{"api://microsoft.durabletask.private/.default"},
		TenantID:  tenantId,
	}
	if len(scopes) > 0 {
		tokenOptions.Scopes = scopes
	}

	credential := azureGrpcCredentials{
		TokenOptions:        tokenOptions,
		disableTokenRefresh: disableTokenRefresh,
		logger:              logger,
		token:               nil,
		azureCredential:     cred,
		insecure:            insecure,
	}

	// grab a token and start the token refresh goroutine
	tokenErr := credential.UpdateToken(ctx)
	if tokenErr != nil {
		return azureGrpcCredentials{logger: logger}, fmt.Errorf("failed to get access token: %v", tokenErr)
	}

	if !disableTokenRefresh {
		go func() {
			for {
				select {
				case <-time.After(time.Until(credential.GetTokenRefreshTime())):
					logger.Info("Refreshing access token")
					tokenErr := credential.UpdateToken(ctx)
					if tokenErr != nil {
						logger.Errorf("failed to update access token: %v", tokenErr)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return credential, nil
}

func (c azureGrpcCredentials) GetTokenRefreshTime() time.Time {
	if c.token == nil {
		return time.Now()
	}
	// we refresh the token 2 minutes before it expires
	return c.token.ExpiresOn.Add(-2 * time.Minute)
}

func (c azureGrpcCredentials) GetToken() string {
	if c.token == nil {
		c.logger.Debug("Token is nil. Returning empty string.")
		return ""
	}
	c.logger.Debug("Token: %s", c.token.Token)
	return c.token.Token
}

func (c *azureGrpcCredentials) UpdateToken(ctx context.Context) error {
	c.logger.Debug("Updating token")
	token, tokenErr := c.azureCredential.GetToken(ctx, c.TokenOptions)
	if tokenErr == nil {
		c.token = &token
	}

	return tokenErr
}

func (c azureGrpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": "Bearer " + c.GetToken(),
	}, nil
}

func (c azureGrpcCredentials) RequireTransportSecurity() bool {
	return c.insecure
}

type taskHubCredential struct {
	taskhub  string
	insecure bool
}

func (c taskHubCredential) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"taskhub": c.taskhub,
	}, nil
}

func (c taskHubCredential) RequireTransportSecurity() bool {
	return c.insecure
}

func newTaskHubCredential(taskhub string, insecure bool) taskHubCredential {
	return taskHubCredential{
		taskhub:  taskhub,
		insecure: insecure,
	}
}

func CreateGrpcDialOptions(ctx context.Context, logger backend.Logger, isInsecure bool, isAuthDisabled bool, taskHubName string, userAgent string, azureCredential *azcore.TokenCredential, resourceScopes []string, tenantID string) ([]grpc.DialOption, error) {
	var grpcDialOptions []grpc.DialOption = []grpc.DialOption{
		grpc.WithBlock(),
	}
	// Authentication options
	if isInsecure {
		grpcDialOptions = append(grpcDialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// set TLS grpc transport security
		creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: false})
		grpcDialOptions = append(grpcDialOptions, grpc.WithTransportCredentials(creds))
	}
	if !isAuthDisabled {
		creds, err := newAzureGrpcCredentials(ctx, *azureCredential, resourceScopes, tenantID, logger, false, isInsecure)
		if err != nil {
			logger.Error("failed to get azure credentials: ", err)
			return nil, fmt.Errorf("failed to get azure credentials: %v", err)
		}
		grpcDialOptions = append(grpcDialOptions, grpc.WithPerRPCCredentials(creds))
	}

	// End of Authentication options
	// Attach required metadata on every request, for example the taskhub Name
	taskHubCredential := newTaskHubCredential(taskHubName, isInsecure)
	grpcDialOptions = append(grpcDialOptions, grpc.WithPerRPCCredentials(taskHubCredential))
	grpcDialOptions = append(grpcDialOptions, grpc.WithUserAgent(userAgent))
	// End of required metadata

	// check whether environment variable GRPC_INTERCEPTOR_LOGGING is set to true
	loggerEnv, found := os.LookupEnv("GRPC_INTERCEPTOR_LOGGING")
	if found && (loggerEnv == "true" || loggerEnv == "1") {
		opts := []logging.Option{
			logging.WithLogOnEvents(logging.PayloadSent, logging.PayloadReceived),
		}
		grpcDialOptions = append(grpcDialOptions, grpc.WithUnaryInterceptor(logging.UnaryClientInterceptor(InterceptorLogger(logger), opts...)))
		grpcDialOptions = append(grpcDialOptions, grpc.WithStreamInterceptor(logging.StreamClientInterceptor(InterceptorLogger(logger), opts...)))
	}
	return grpcDialOptions, nil
}
