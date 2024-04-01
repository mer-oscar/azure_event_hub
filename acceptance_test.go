package azure_event_hub

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/kelseyhightower/envconfig"
	"github.com/matryer/is"
	"github.com/prongbang/goenv"
	"go.uber.org/goleak"
)

type envConfig struct {
	AzureTenantID       string `envconfig:"AZURE_TENANT_ID"`
	AzureClientID       string `envconfig:"AZURE_CLIENT_ID"`
	AzureClientSecret   string `envconfig:"AZURE_CLIENT_SECRET"`
	AzureSubscriptionID string `envconfig:"AZURE_SUBSCRIPTION_ID"`
}

func TestAcceptance(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	err := goenv.LoadEnv("local.env")
	is.NoErr(err)

	var env envConfig
	err = envconfig.Process("", &env)
	is.NoErr(err)

	// make a new event hub
	cred, err := azidentity.NewClientSecretCredential(env.AzureTenantID, env.AzureClientID, env.AzureClientSecret, nil)
	is.NoErr(err)

	eventhubAcceptance := "acceptance-test"
	rgn, ns := "test-connector", "connector-eh"
	fqns := ns + ".servicebus.windows.net"

	cfg := map[string]string{
		"azure.tenantId":     env.AzureTenantID,
		"azure.clientId":     env.AzureClientID,
		"azure.clientSecret": env.AzureClientSecret,
		"eventHubName":       eventhubAcceptance,
		"eventHubNamespace":  fqns,
	}

	clientFactory, err := armeventhub.NewClientFactory(env.AzureSubscriptionID, cred, nil)
	is.NoErr(err)

	_, err = clientFactory.NewEventHubsClient().CreateOrUpdate(ctx, rgn, ns, eventhubAcceptance, armeventhub.Eventhub{
		Properties: &armeventhub.Properties{
			MessageRetentionInDays: to.Ptr[int64](1),
			PartitionCount:         to.Ptr[int64](4),
			Status:                 to.Ptr(armeventhub.EntityStatusActive),
		},
	}, nil)
	is.NoErr(err)

	// delete event hub
	defer func() {
		_, err = clientFactory.NewEventHubsClient().Delete(ctx, rgn, ns, eventhubAcceptance, nil)
		is.NoErr(err)
	}()

	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector:         Connector,
			GoleakOptions:     []goleak.Option{goleak.IgnoreCurrent()},
			SourceConfig:      cfg,
			DestinationConfig: cfg,
			GenerateDataType:  sdk.GenerateRawData,
			Skip: []string{
				"TestSource_Configure_RequiredParams",
				"TestDestination_Configure_RequiredParams",
			},
			WriteTimeout: 5000 * time.Millisecond,
			ReadTimeout:  5000 * time.Millisecond,
		},
	})
}
