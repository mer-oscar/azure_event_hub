package destination

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/kelseyhightower/envconfig"
	"github.com/matryer/is"
	"github.com/prongbang/goenv"
)

type envConfig struct {
	AzureTenantID       string `envconfig:"AZURE_TENANT_ID"`
	AzureClientID       string `envconfig:"AZURE_CLIENT_ID"`
	AzureClientSecret   string `envconfig:"AZURE_CLIENT_SECRET"`
	AzureSubscriptionID string `envconfig:"AZURE_SUBSCRIPTION_ID"`
}

func Test_Destination(t *testing.T) {
	eventhubDest := "destination-integration-test"
	rgn, ns := "test-connector", "connector-eh"
	is := is.New(t)

	ctx := context.Background()
	err := goenv.LoadEnv("local.env")
	is.NoErr(err)

	var env envConfig
	err = envconfig.Process("", &env)
	is.NoErr(err)

	// // make a new event hub
	cred, err := azidentity.NewClientSecretCredential(env.AzureTenantID, env.AzureClientID, env.AzureClientSecret, nil)
	is.NoErr(err)

	clientFactory, err := armeventhub.NewClientFactory(env.AzureSubscriptionID, cred, nil)
	is.NoErr(err)

	_, err = clientFactory.NewEventHubsClient().CreateOrUpdate(ctx, rgn, ns, eventhubDest, armeventhub.Eventhub{
		Properties: &armeventhub.Properties{
			MessageRetentionInDays: to.Ptr[int64](1),
			PartitionCount:         to.Ptr[int64](4),
			Status:                 to.Ptr(armeventhub.EntityStatusActive),
		},
	}, nil)
	is.NoErr(err)

	fqns := ns + ".servicebus.windows.net"

	config := map[string]string{
		"azure.tenantId":     env.AzureTenantID,
		"azure.clientId":     env.AzureClientID,
		"azure.clientSecret": env.AzureClientSecret,
		"eventHubName":       eventhubDest,
		"eventHubNamespace":  fqns,
	}

	con := New()
	err = con.Configure(ctx, config)
	is.NoErr(err)

	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
		// delete event hub
		_, err = clientFactory.NewEventHubsClient().Delete(ctx, rgn, ns, eventhubDest, nil)
		is.NoErr(err)
	}()

	// open
	err = con.Open(ctx)
	is.NoErr(err)

	// prepare recs
	recs := make([]sdk.Record, 0)
	for i := range 10 {
		rec := sdk.Util.Source.NewRecordCreate(
			sdk.Position{},
			nil,
			nil,
			sdk.RawData(fmt.Sprintf("record %d", i)),
		)
		recs = append(recs, rec)
	}

	// write
	count, err := con.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(count, 10)
}
