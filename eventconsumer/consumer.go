package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"reconciliation/dlt"
	"reconciliation/model"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	msgCh  chan kafka.Message
	wg     sync.WaitGroup
}

// FabricConfig holds the configuration for Hyperledger Fabric
type FabricConfig struct {
}

func NewConsumer(brokers []string, groupID, topic string, bufferSize int) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			Topic:          topic,
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			CommitInterval: time.Second,
		}),
		msgCh: make(chan kafka.Message, bufferSize),
	}
}

func (c *Consumer) Start(ctx context.Context, workerCount int, config *model.Config) {

	// Start workers to process messages
	for i := 0; i < workerCount; i++ {
		c.wg.Add(1)
		go c.processMessages(ctx, i, config)
	}

	// Read messages from Kafka
	go c.readMessages(ctx)

	// Wait for all workers to finish
	c.wg.Wait()
}

func (c *Consumer) readMessages(ctx context.Context) {
	defer close(c.msgCh)
	for {
		// Check if context is done
		select {
		case <-ctx.Done():
			log.Println("Stopping message reading due to context cancellation")
			return
		default:
			// Attempt to read a message from Kafka
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Error reading message: %v", err)
				}
				return
			}
			// Send the message to the processing channel
			c.msgCh <- msg
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, workerID int, config *model.Config) {

	clientConnection := dlt.NewGrpcConnection(config)
	defer clientConnection.Close()

	id := dlt.NewIdentity(config)
	sign := dlt.NewSign(config)

	// Create a Gateway connection for a specific client identity
	gw, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithClientConnection(clientConnection),
		// Default timeouts for different gRPC calls
		client.WithEvaluateTimeout(5*time.Second),
		client.WithEndorseTimeout(15*time.Second),
		client.WithSubmitTimeout(5*time.Second),
		client.WithCommitStatusTimeout(1*time.Minute),
	)
	if err != nil {
		panic(err)
	}
	defer gw.Close()

	network := gw.GetNetwork(config.Dlt.ChannelID)
	contract := network.GetContract(config.Dlt.ChaincodeID)

	log.Printf("contract contract %v", config)

	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: Stopping due to context cancellation", workerID)
			return
		case msg, ok := <-c.msgCh:
			if !ok {
				log.Printf("Worker %d: Message channel closed", workerID)
				return
			}
			// Process the message
			c.handleMessage(msg, workerID, contract)
		}
	}
}

func (c *Consumer) handleMessage(msg kafka.Message, workerID int, contract *client.Contract) {
	log.Printf("Worker %d: Processing message offset %d: key=%s value=%s\n", workerID, msg.Offset, string(msg.Key), string(msg.Value))

	_, err := contract.SubmitTransaction("loadTrans", string(msg.Value))
	if err != nil {
		panic(fmt.Errorf("failed to submit transaction: %w", err))
	}

	fmt.Printf("*** Transaction committed successfully\n")

	// Simulate processing time
	time.Sleep(500 * time.Millisecond)
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		log.Printf("Failed to close Kafka reader: %v", err)
	}
}
