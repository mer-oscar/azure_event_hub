package source

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
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

func TestRead_Snapshot_CDC(t *testing.T) {
	eventhubSource := "source-integration-test"
	rgn, ns := "test-connector", "connector-eh"
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

	clientFactory, err := armeventhub.NewClientFactory(env.AzureSubscriptionID, cred, nil)
	is.NoErr(err)

	_, err = clientFactory.NewEventHubsClient().CreateOrUpdate(ctx, rgn, ns, eventhubSource, armeventhub.Eventhub{
		Properties: &armeventhub.Properties{
			MessageRetentionInDays: to.Ptr[int64](1),
			PartitionCount:         to.Ptr[int64](4),
			Status:                 to.Ptr(armeventhub.EntityStatusActive),
		},
	}, nil)
	is.NoErr(err)

	fqns := ns + ".servicebus.windows.net"

	// write records to it
	pClient, err := azeventhubs.NewProducerClient(fqns, eventhubSource, cred, nil)
	is.NoErr(err)

	batch, err := pClient.NewEventDataBatch(ctx, nil)
	is.NoErr(err)

	for i := range 10 {
		err := batch.AddEventData(&azeventhubs.EventData{
			Body:        []byte(fmt.Sprintf("record %d", i)),
			ContentType: to.Ptr("application/json"),
		}, nil)
		is.NoErr(err)
	}

	err = pClient.SendEventDataBatch(ctx, batch, nil)
	is.NoErr(err)

	config := map[string]string{
		"azure.tenantId":     env.AzureTenantID,
		"azure.clientId":     env.AzureClientID,
		"azure.clientSecret": env.AzureClientSecret,
		"eventHubName":       eventhubSource,
		"eventHubNamespace":  fqns,
	}

	con := New()
	err = con.Configure(ctx, config)
	is.NoErr(err)

	// open
	err = con.Open(ctx, nil)
	is.NoErr(err)

	// snapshot
	var position sdk.Position
	for i := range 10 {
		rec, err := con.Read(ctx)
		is.NoErr(err)

		if i == 9 {
			position = rec.Position
		}
	}

	// close
	err = con.Teardown(ctx)
	is.NoErr(err)

	// open
	err = con.Open(ctx, position)
	is.NoErr(err)

	// cdc
	go func() {
		go func() {
			batch, err := pClient.NewEventDataBatch(ctx, nil)
			is.NoErr(err)

			for i := range 5 {
				err := batch.AddEventData(&azeventhubs.EventData{
					Body:        []byte(fmt.Sprintf("record %d", i+15)),
					ContentType: to.Ptr("application/json"),
				}, nil)
				is.NoErr(err)
			}

			err = pClient.SendEventDataBatch(ctx, batch, nil)
			is.NoErr(err)
		}()

		batch, err := pClient.NewEventDataBatch(ctx, nil)
		is.NoErr(err)

		for i := range 5 {
			err := batch.AddEventData(&azeventhubs.EventData{
				Body:        []byte(fmt.Sprintf("record %d", i+10)),
				ContentType: to.Ptr("application/json"),
			}, nil)
			is.NoErr(err)
		}

		err = pClient.SendEventDataBatch(ctx, batch, nil)
		is.NoErr(err)
	}()

	var counter int
	for {
		if counter == 10 {
			break
		}

		_, err := con.Read(ctx)
		is.NoErr(err)

		counter++
	}

	// block for a bit so that the messages can batch and send while the read
	<-time.After(time.Millisecond * 1300)

	// close
	err = con.Teardown(ctx)
	is.NoErr(err)

	// delete event hub
	_, err = clientFactory.NewEventHubsClient().Delete(ctx, rgn, ns, eventhubSource, nil)
	is.NoErr(err)
}
