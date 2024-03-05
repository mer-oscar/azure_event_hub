package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config           Config
	client           *azeventhubs.ConsumerClient
	readError        chan error
	readBuffer       chan sdk.Record
	partitionClients []*azeventhubs.PartitionClient
}

type EventPosition struct {
	SequenceNumber int64
	Offset         int64
}

func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		readError:  make(chan error, 1),
		readBuffer: make(chan sdk.Record, 1000),
	}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	defaultAzureCred, err := azidentity.NewClientSecretCredential(s.config.AzureTenantID, s.config.AzureClientID, s.config.AzureClientSecret, nil)
	if err != nil {
		return err
	}

	s.client, err = azeventhubs.NewConsumerClient(s.config.EventHubNameSpace, s.config.EventHubName, azeventhubs.DefaultConsumerGroup, defaultAzureCred, nil)
	if err != nil {
		return err
	}

	ehProps, err := s.client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return err
	}

	var startPosition azeventhubs.StartPosition
	startPosition.Earliest = to.Ptr[bool](true)

	if pos != nil {
		startPosition.Earliest = to.Ptr[bool](false)
		startPosition.Inclusive = true
		startPosition.SequenceNumber = to.Ptr[int64](24)
	}

	for _, partitionID := range ehProps.PartitionIDs {
		partitionClient, err := s.client.NewPartitionClient(partitionID, &azeventhubs.PartitionClientOptions{
			StartPosition: startPosition,
		})
		if err != nil {
			return err
		}

		s.partitionClients = append(s.partitionClients, partitionClient)
	}

	go s.dispatchPartitionClients(ctx)

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	select {
	case err := <-s.readError:
		if err != nil {
			return sdk.Record{}, err
		}
		return sdk.Record{}, ctx.Err()
	case rec := <-s.readBuffer:
		return rec, nil
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	for _, client := range s.partitionClients {
		if client != nil {
			client.Close(ctx)
		}
	}

	if s.client != nil {
		err := s.client.Close(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Source) dispatchPartitionClients(ctx context.Context) {
	for _, client := range s.partitionClients {
		client := client
		go func() {
			for {
				select {
				case err := <-s.readError:
					if err != nil {
						// log error and close out
						sdk.Logger(ctx).Err(err).Msg("error receiving events")
						return
					}
					break
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond * 500):
					// Wait up to a 500ms for 1000 events, otherwise returns whatever we collected during that time.
					receiveCtx, cancelReceive := context.WithTimeout(ctx, time.Millisecond*500)
					events, err := client.ReceiveEvents(receiveCtx, 1000, nil)
					cancelReceive()

					if err != nil && !errors.Is(err, context.DeadlineExceeded) {
						s.readError <- err
						return
					}

					if len(events) == 0 {
						continue
					}

					for _, event := range events {
						position := EventPosition{
							SequenceNumber: event.SequenceNumber,
							Offset:         event.Offset,
						}

						posBytes, err := json.Marshal(position)
						if err != nil {
							s.readError <- err
							return
						}

						rec := sdk.Util.Source.NewRecordCreate(
							sdk.Position(posBytes),
							nil,
							sdk.RawData(*event.MessageID),
							sdk.RawData(event.Body))

						s.readBuffer <- rec
					}
				}
			}
		}()
	}
}

func parsePosition(pos sdk.Position) (EventPosition, error) {
	var eventPos EventPosition
	err := json.Unmarshal(pos, &eventPos)
	if err != nil {
		return eventPos, err
	}

	return eventPos, nil
}
