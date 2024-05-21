package durabletaskservice

import (
	"fmt"
	"net"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

const (
	// DefaultEndpoint is the default endpoint for the DurableTaskServiceBackend service.
	defaultEndpoint = "localhost:5147"
	// orchestratorHistoryCacheSize is the default size of the orchestrator history cache.
	defaultOrchestrationHistoryCacheSize int = 100
	// DefaultPort is the default port for the DurableTaskServiceBackend service.
	defaultPort int = 443
	// DefaultUserAgent is the default user agent for the DurableTaskServiceBackend service.
	baseUserAgent string = "durabetask-go"
)

type durableTaskServiceBackendOptions struct {
	Endpoint                      string
	TaskHubName                   string
	ResourceScopes                []string
	TenantID                      string
	ClientID                      string
	AzureCredential               azcore.TokenCredential
	DisableAuth                   bool
	Orchestrators                 []string
	Activities                    []string
	Insecure                      bool
	OrchestrationHistoryCacheSize int
	UserAgent                     string
}

type DurableTaskServiceBackendConfigurationOption func(*durableTaskServiceBackendOptions)

func normalizeEndpoint(endpoint string) string {
	if endpoint == "" {
		endpoint = defaultEndpoint
	} else {
		protocolParts := strings.Split("://", endpoint)
		if len(protocolParts) == 2 {
			endpoint = protocolParts[1]
		}

		_, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			endpoint = fmt.Sprintf("%s:%d", endpoint, defaultPort)
		}
	}
	return endpoint
}

func newDurableTaskServiceBackendConfiguration(endpoint string, taskHubName string, options ...DurableTaskServiceBackendConfigurationOption) (*durableTaskServiceBackendOptions, error) {
	opts := &durableTaskServiceBackendOptions{
		Endpoint:                      endpoint,
		TaskHubName:                   taskHubName,
		ResourceScopes:                []string{},
		AzureCredential:               nil,
		TenantID:                      "",
		ClientID:                      "",
		DisableAuth:                   false,
		Insecure:                      false,
		OrchestrationHistoryCacheSize: defaultOrchestrationHistoryCacheSize,
		UserAgent:                     baseUserAgent,
	}

	if opts.TaskHubName == "" {
		opts.TaskHubName = "default"
	}

	// normalize the endpoint

	opts.Endpoint = normalizeEndpoint(endpoint)

	// Apply all options

	for _, option := range options {
		option(opts)
	}

	// If no AzureCredential is provided, use the default AzureCredential
	if opts.AzureCredential == nil {
		var err error
		opts.AzureCredential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
	}

	return opts, nil
}

func WithDisableAuth() DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.DisableAuth = true
	}
}

func WithUserAgent(userAgent string) DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.UserAgent = fmt.Sprintf("%s/%s", userAgent, baseUserAgent)
	}
}

func WithInsecureMode() DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.Insecure = true
	}
}

func WithAzureTenantID(tenantID string) DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.TenantID = tenantID
	}
}

func WithAzureClientID(clientID string) DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.ClientID = clientID
	}
}

func WithAzureResourceScopes(resourceScopes []string) DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.ResourceScopes = resourceScopes
	}
}

func WithOrchestrationHistoryCacheSize(size int) DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		o.OrchestrationHistoryCacheSize = size
	}
}

func WithCredential(credential azcore.TokenCredential) DurableTaskServiceBackendConfigurationOption {
	return func(o *durableTaskServiceBackendOptions) {
		if credential == nil {
			var err error
			credential, err = azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return
			}
		}
		o.AzureCredential = credential
	}
}
