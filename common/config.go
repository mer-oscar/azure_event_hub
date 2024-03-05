package common

type Config struct {
	// AzureTenantID is the azure tenant ID
	AzureTenantID string `json:"azure.tenantId" validate:"required"`
	// AzureClientID is the azure client ID
	AzureClientID string `json:"azure.clientId" validate:"required"`
	// AzureClientSecret is the azure client secret
	AzureClientSecret string `json:"azure.clientSecret" validate:"required"`

	// EventHubNameSpace is the FQNS of your event hubs resource
	EventHubNameSpace string `json:"eventHubNamespace" validate:"required"`
	// EventHubName is your event hub name- analogous to a kafka topic
	EventHubName string `json:"eventHubName" validate:"required"`
}
