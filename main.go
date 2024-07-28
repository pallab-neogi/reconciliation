package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"reconciliation/dlt"
	consumer "reconciliation/eventconsumer"
	"reconciliation/model"
	postgresdb "reconciliation/postgres"
	"reconciliation/transformer"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/cenkalti/backoff/v4"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Message struct {
	Topic   string
	Key     string
	Payload []byte
}

func main() {

	fmt.Println("Starting Reconciliation job . Version ")
	defer fmt.Println("Shutdown Reconciliation complete....")

	action := flag.String("a", "", "recon-producer|recon-consumer|recon1-updatetrans|recon-unmatchtrans|reconciliation1|reconciliation2|reconciliation3")
	configname := flag.String("config", "", "config-recon1|config-recon2|config-recon3")
	startDate := flag.String("start", "", "Start date")
	endDate := flag.String("end", "", "End Date")
	docType := flag.String("doctype", "", "docType")

	flag.Parse()

	// Load configuration
	config, err := model.InitConfig(*configname)
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	logrus.SetFormatter(&logrus.JSONFormatter{})

	//args := flag.Args()

	switch *action {
	case "recon-producer":
		{

			logrus.Infof("action %v", *action)

			logrus.Infof("config %v", config.Azure.AccountName)

			// Create channels for CSV records and error handling
			recordsChan := make(chan Message, config.CSV.ChunkSize)
			errorChan := make(chan error, 1) // Buffer to ensure error messages are not missed
			done := make(chan struct{})

			// Create a wait group to synchronize goroutines
			var wg sync.WaitGroup

			// Start the Kafka sending goroutine
			wg.Add(1)
			go sendToKafka(config.Kafka.Broker, config.Kafka.Topic, recordsChan, errorChan, done, &wg, config.Retry.MaxRetries, config.Retry.RetryInterval)

			// Download the CSV from Azure and process it line by line
			err = downloadAndProcessCSVFromAzure(config, func(record []string, dirName string) {
				select {
				case <-done:
					logrus.Info("Processing received shutdown signal")
					return
				default:

					logrus.Infof("Retrieved Kafka record %s", record)

					jsonData, id, _ := transformer.TransformRecord(record, dirName, config.Kafka.Topic)

					if jsonData != nil {
						recordsChan <- Message{Topic: config.Kafka.Topic, Key: id, Payload: jsonData}
					}

					logrus.Infof("Successfully sent records to Kafka %s", dirName)
				}
			})
			if err != nil {
				log.Fatalf("Error downloading and processing CSV from Azure: %v", err)
			}

			close(recordsChan)

			// Setup graceful shutdown
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

			go func() {
				<-sigs
				logrus.Info("Received shutdown signal, closing channels and waiting for goroutines to finish...")
				close(done)
			}()

			// Wait for both goroutines to finish
			go func() {
				wg.Wait()
				close(errorChan)
			}()

			// Handle errors and wait for completion
			for err := range errorChan {
				if err != nil {
					logrus.Errorf("Error Details: %v", err)
				}
			}

		}

	case "recon-consumer":
		{
			// Create a new Kafka consumer
			consumer := consumer.NewConsumer([]string{config.Kafka.Broker}, config.Kafka.GroupID, config.Kafka.Topic, 100)

			// Create a context with cancellation
			ctx, cancel := context.WithCancel(context.Background())

			// Signal handling for graceful shutdown
			sigchan := make(chan os.Signal, 1)
			signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

			go func() {
				<-sigchan
				log.Println("Received interrupt signal. Initiating shutdown...")
				cancel()
			}()

			// Start the consumer with 3 worker goroutines
			go consumer.Start(ctx, 1, config)

			// Wait for the context to be canceled
			<-ctx.Done()

			// Ensure the consumer is properly closed
			consumer.Close()

			logrus.Info("Consumer has shut down gracefully")

		}

	case "reconciliation1":
		{

			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			fmt.Printf("Connection String : %v", connStr)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Channel to receive the queried users
			userChannel := make(chan []byte)
			errChannel := make(chan error)

			// Start a goroutine to query Hyperledger data
			go func() {

				fmt.Printf("matchedTransByte matchedTransByte : %v %v", *startDate, *endDate)
				matchedTransByte, err := dlt.QueryTrans(config, "Reconciliation", *startDate, *endDate, "")

				fmt.Printf("matchedTransByte matchedTransByte : %v", string(matchedTransByte))

				if err != nil {
					errChannel <- err
					return
				}
				userChannel <- matchedTransByte
			}()

			// Handle the results
			select {
			case matchedTransByte := <-userChannel:
				// Load data into PostgreSQL

				if err := postgresdb.BulkInsertTrans(pool, matchedTransByte, "match_trans"); err != nil {
					log.Fatalf("Failed to insert users: %v", err)
				}
				log.Println("Successfully loaded data into the database!")

			case err := <-errChannel:
				log.Fatalf("Failed to query Hyperledger data: %v", err)
			}

		}

	case "recon1-updatetrans":
		{
			startTime, err := time.Parse(time.RFC3339, *startDate)
			if err != nil {
				log.Fatalf("Failed to parse start date: %v\n", err)
			}

			endTime, err := time.Parse(time.RFC3339, *endDate)
			if err != nil {
				log.Fatalf("Failed to parse end date: %v\n", err)
			}

			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Query data
			rows, err := pool.Query(context.Background(), "SELECT id  FROM public.match_trans WHERE transdatetime >= $1 and transdatetime <=$2 ", startTime, endTime)
			if err != nil {
				log.Fatalf("Failed to query data: %v\n", err)
			}
			defer rows.Close()

			DLTId := ""

			for rows.Next() {
				var id string
				err := rows.Scan(&id)
				if err != nil {
					log.Fatalf("Failed to scan row: %v\n", err)
				}
				if len(DLTId) == 0 {
					DLTId = id
				} else {
					DLTId = DLTId + "," + id
				}

			}

			log.Printf("DLTIds %v", DLTId)

			matchedTransByte, err := dlt.UpdateTrans(config, DLTId, transformer.GetCurrentTime())

			if err != nil {
				log.Printf("Failed to update DLT ID for recon1-updatetrans: %v\n", err)
			}

			log.Printf("DLT update status %v", string(matchedTransByte))

		}
	/*case "recon1-unmatchtrans":
	{
		connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

		fmt.Printf("Connection String : %v", connStr)

		// Create PostgreSQL connection pool
		pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
		if err != nil {
			log.Fatalf("Failed to create connection pool: %v", err)
		}
		defer pool.Close()

		// Channel to receive the queried users
		userChannel := make(chan []byte)
		errChannel := make(chan error)

		// Start a goroutine to query Hyperledger data
		go func() {

			unmatchedTransByte, err := dlt.QueryTrans(config, "queryUnmatchTrans", *startDate, *endDate, *docType)

			fmt.Printf("matchedTransByte matchedTransByte : %v", string(unmatchedTransByte))

			if err != nil {
				errChannel <- err
				return
			}
			userChannel <- unmatchedTransByte
		}()

		// Handle the results
		select {
		case unmatchedTransByte := <-userChannel:
			// Load data into PostgreSQL

			if err := postgresdb.BulkInsertUnmatchTrans(pool, unmatchedTransByte, *docType, config.Kafka.Topic); err != nil {
				log.Fatalf("Failed to insert unmatch transaction: %v", err)
			}
			log.Println("Successfully loaded data into the Postgres!")

		case err := <-errChannel:
			log.Fatalf("Failed to query Hyperledger data: %v", err)
		}
	}*/

	case "reconciliation2":
		{

			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			fmt.Printf("Connection String : %v", connStr)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Channel to receive the queried users
			userChannel := make(chan []byte)
			errChannel := make(chan error)

			// Start a goroutine to query Hyperledger data
			go func() {

				fmt.Printf("matchedTransByte matchedTransByte : %v %v", *startDate, *endDate)
				matchedTransByte, err := dlt.QueryTrans(config, "ReconciliationRecon2", *startDate, *endDate, "")

				fmt.Printf("matchedTransByte matchedTransByte : %v", string(matchedTransByte))

				if err != nil {
					errChannel <- err
					return
				}
				userChannel <- matchedTransByte
			}()

			// Handle the results
			select {
			case matchedTransByte := <-userChannel:
				// Load data into PostgreSQL

				if err := postgresdb.BulkInsertTrans(pool, matchedTransByte, "match_trans_recon2"); err != nil {
					log.Fatalf("Failed to insert users: %v", err)
				}
				log.Println("Successfully loaded data into the database!")

			case err := <-errChannel:
				log.Fatalf("Failed to query Hyperledger data: %v", err)
			}

		}

	case "reconciliation3":
		{

			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			fmt.Printf("Connection String : %v", connStr)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Channel to receive the queried users
			userChannel := make(chan []byte)
			errChannel := make(chan error)

			// Start a goroutine to query Hyperledger data
			go func() {

				fmt.Printf("matchedTransByte matchedTransByte : %v %v", *startDate, *endDate)
				matchedTransByte, err := dlt.QueryTrans(config, "ReconciliationRecon3", *startDate, *endDate, "")

				fmt.Printf("matchedTransByte matchedTransByte : %v", string(matchedTransByte))

				if err != nil {
					errChannel <- err
					return
				}
				userChannel <- matchedTransByte
			}()

			// Handle the results
			select {
			case matchedTransByte := <-userChannel:
				// Load data into PostgreSQL

				log.Printf("matchedTransBytematchedTransByte %v", string(matchedTransByte))

				if err := postgresdb.BulkInsertTrans(pool, matchedTransByte, "match_trans_recon3"); err != nil {
					log.Fatalf("Failed to insert users: %v", err)
				}
				log.Println("Successfully loaded data into the database!")

			case err := <-errChannel:
				log.Fatalf("Failed to query Hyperledger data: %v", err)
			}

		}

	case "recon2-updatetrans":
		{
			startTime, err := time.Parse(time.RFC3339, *startDate)
			if err != nil {
				log.Fatalf("Failed to parse start date: %v\n", err)
			}

			endTime, err := time.Parse(time.RFC3339, *endDate)
			if err != nil {
				log.Fatalf("Failed to parse end date: %v\n", err)
			}

			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Query data
			rows, err := pool.Query(context.Background(), "SELECT id  FROM match_trans_recon2 WHERE transdatetime >= $1 and transdatetime <=$2", startTime, endTime)
			if err != nil {
				log.Fatalf("Failed to query data: %v\n", err)
			}
			defer rows.Close()

			DLTId := ""

			for rows.Next() {
				var id string
				err := rows.Scan(&id)
				if err != nil {
					log.Fatalf("Failed to scan row: %v\n", err)
				}
				if len(DLTId) == 0 {
					DLTId = id
				} else {
					DLTId = DLTId + "," + id
				}

			}

			log.Printf("DLTIds %v", DLTId)

			matchedTransByte, err := dlt.UpdateTrans(config, DLTId, transformer.GetCurrentTime())

			if err != nil {
				log.Printf("Failed to update DLT ID for recon2-updatetrans: %v\n", err)
			}

			log.Printf("DLT update status %v", string(matchedTransByte))

		}
	case "recon3-updatetrans":
		{
			startTime, err := time.Parse(time.RFC3339, *startDate)
			if err != nil {
				log.Fatalf("Failed to parse start date: %v\n", err)
			}

			endTime, err := time.Parse(time.RFC3339, *endDate)
			if err != nil {
				log.Fatalf("Failed to parse end date: %v\n", err)
			}

			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Query data
			rows, err := pool.Query(context.Background(), "SELECT id  FROM match_trans_recon3 WHERE transdatetime >= $1 and transdatetime <=$2", startTime, endTime)
			if err != nil {
				log.Fatalf("Failed to query data: %v\n", err)
			}
			defer rows.Close()

			DLTId := ""

			for rows.Next() {
				var id string
				err := rows.Scan(&id)
				if err != nil {
					log.Fatalf("Failed to scan row: %v\n", err)
				}
				if len(DLTId) == 0 {
					DLTId = id
				} else {
					DLTId = DLTId + "," + id
				}

			}

			log.Printf("DLTIds %v", DLTId)

			matchedTransByte, err := dlt.UpdateTrans(config, DLTId, transformer.GetCurrentTime())

			if err != nil {
				log.Printf("Failed to update DLT ID for recon2-updatetrans: %v\n", err)
			}

			log.Printf("DLT update  %v", string(matchedTransByte))

		}
	case "recon-unmatchtrans":
		{
			connStr := fmt.Sprintf("postgres://%s:%s@%s/reconcile", config.Postgres.User, config.Postgres.Password, config.Postgres.Server)

			fmt.Printf("Connection String : %v", connStr)

			// Create PostgreSQL connection pool
			pool, err := postgresdb.CreatePostgresConnectionPool(connStr)
			if err != nil {
				log.Fatalf("Failed to create connection pool: %v", err)
			}
			defer pool.Close()

			// Channel to receive the queried users
			userChannel := make(chan []byte)
			errChannel := make(chan error)

			// Start a goroutine to query Hyperledger data
			go func() {

				unmatchedTransByte, err := dlt.QueryTrans(config, "queryUnmatchTrans", *startDate, *endDate, *docType)

				fmt.Printf("matchedTransByte matchedTransByte : %v", string(unmatchedTransByte))

				if err != nil {
					errChannel <- err
					return
				}
				userChannel <- unmatchedTransByte
			}()

			// Handle the results
			select {
			case unmatchedTransByte := <-userChannel:
				// Load data into PostgreSQL

				if err := postgresdb.BulkInsertUnmatchTrans(pool, unmatchedTransByte, *docType, config.Kafka.Topic); err != nil {
					log.Fatalf("Failed to insert unmatch transaction: %v", err)
				}
				log.Println("Successfully loaded data into the Postgres!")

			case err := <-errChannel:
				log.Fatalf("Failed to query Hyperledger data: %v", err)
			}
		}

	}

}

// Download CSV file from Azure File Storage and process it line by line
func downloadAndProcessCSVFromAzure(config *model.Config, process func([]string, string)) error {

	//-------------------------------------------------
	var IsFirstRecord bool
	serviceURL := fmt.Sprintf("https://%s.file.core.windows.net/", config.Azure.AccountName)
	cred, err := service.NewSharedKeyCredential(config.Azure.AccountName, config.Azure.AccountKey)

	if err != nil {
		log.Fatalf("Failed to connect Azure Account: %v", err)
	}

	client, err := service.NewClientWithSharedKeyCredential(serviceURL, cred, nil)

	if err != nil {
		log.Fatalf("Failed to create service client: %v", err)
	}

	shareClient := client.NewShareClient(config.Azure.ShareName)

	for eachDir, prefix := range config.Azure.DirectoryPath {

		eachDir = strings.ToUpper(eachDir)
		fmt.Printf("Key: %s Value %s \n", eachDir, prefix)

		dirClient := shareClient.NewDirectoryClient(eachDir)
		IsFirstRecord = true
		options := directory.ListFilesAndDirectoriesOptions{
			Prefix:     to.Ptr(prefix),    // prefix to be added
			MaxResults: to.Ptr(int32(10)), // Limit the results
		}

		pager := dirClient.NewListFilesAndDirectoriesPager(&options)

		for pager.More() {
			page, err := pager.NextPage(context.Background())
			if err != nil {
				log.Fatalf("Failed to list files and directories: %v", err)
			}

			// Print the file and directory names
			for _, fileItem := range page.Segment.Files {

				ctx := context.Background()

				IsFirstRecord = true
				fmt.Printf("File: %s\n", *fileItem.Name)

				fileClient := dirClient.NewFileClient(*fileItem.Name)

				props, err := fileClient.GetProperties(ctx, nil)
				if err != nil {
					log.Fatalf("Failed to get file properties: %v", err)
				}

				// Create a buffer to hold the file's content
				buffer := make([]byte, *props.ContentLength)

				// Define download options with ChunkSize
				downloadOptions := file.DownloadBufferOptions{
					// Example to set a specific chunk size
					ChunkSize:   1024 * 1024, // 1 MB chunks
					Concurrency: 4,           // Number of parallel connections
				}

				// Download the file content into the buffer
				_, err = fileClient.DownloadBuffer(ctx, buffer, &downloadOptions)
				if err != nil {
					log.Fatalf("Failed to download file: %v", err)
				}

				// Convert the buffer to a string and parse it as CSV
				csvReader := csv.NewReader(strings.NewReader(string(buffer)))

				// Read and parse the CSV data
				records, err := csvReader.ReadAll()
				if err != nil {
					log.Fatalf("Failed to parse CSV data: %v", err)
				}

				// Print the CSV records
				for _, record := range records {

					if eachDir == "MARKOFFREJ" || eachDir == "MARKOFFACC" || eachDir == "GL" {
						if !IsFirstRecord {
							process(record, eachDir)
						}
					} else {
						process(record, eachDir)
					}

					IsFirstRecord = false
				}

				// Move the file into Backup folder.

				//destinationFileClient := shareClient.NewRootDirectoryClient().NewFileClient(eachDir + "/Backup/" + *fileItem.Name)

				destinationFileClient := dirClient.NewSubdirectoryClient("Backup").NewFileClient(*fileItem.Name)

				fmt.Printf("File: %s\n", destinationFileClient.URL())

				fmt.Printf("File: %v\n", destinationFileClient)

				_, err = destinationFileClient.StartCopyFromURL(ctx, fileClient.URL(), nil)
				if err != nil {
					log.Fatalf("Failed to start copy operation: %s", err)
				}

				// Polling until the copy operation is completed
				for {
					props, err := destinationFileClient.GetProperties(ctx, nil)
					if err != nil {
						log.Fatalf("Failed to get properties: %s", err)
					}

					if props.CopyStatus != nil && *props.CopyStatus == file.CopyStatusTypeSuccess {
						fmt.Println("Copy operation completed successfully")
						break
					}

					fmt.Println("Copy operation in progress...")
					time.Sleep(2 * time.Second) // Wait for a few seconds before checking again
				}

				/*	_, err = fileClient.Delete(ctx, nil)

					if err != nil {
						log.Fatalf("Failed to delete file: %s", err)
					}*/

			}

		}
	}

	//-------------------------------------------------

	// Create a service client
	/**

		credential, err := file.NewSharedKeyCredential(config.Azure.AccountName, config.Azure.AccountKey)
	if err != nil {
		return fmt.Errorf("failed to create Azure credentials: %w", err)
	}

	serviceURL := fmt.Sprintf("https://%s.file.core.windows.net", accountName)
	    serviceClient, err := file.SharedKeyCredential(serviceURL,credential,nil)
	    if err != nil {
	        log.Fatalf("Failed to create service client: %v", err)
	    }

		   // Get a client for the file share
		   shareClient := serviceClient.

		   // Get a client for the directory
		   dirClient := shareClient.NewDirectoryClient(directoryPath)

		   // Use context for the request
		   ctx := context.Background()


		fileClient, err1 := file.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.file.core.windows.net/%v%v", config.Azure.AccountName, config.Azure.AccountName, config.Azure.AccountName), credential, nil)
		if err1 != nil {
			return fmt.Errorf("Failed to create service client: %w", err1)
		}

		// Get the file properties to determine its size
		props, err := fileClient.GetProperties(context.Background(), nil)
		if err != nil {
			log.Fatalf("Failed to get file properties: %v", err)
		}

		// Create a buffer to hold the file's content
		buffer := make([]byte, *props.ContentLength)

		// Define download options with ChunkSize
		downloadOptions := file.DownloadBufferOptions{
			// Example to set a specific chunk size
			ChunkSize:   1024 * 1024, // 1 MB chunks
			Concurrency: 4,           // Number of parallel connections
		}

		// Download the file content into the buffer
		_, err = fileClient.DownloadBuffer(context.Background(), buffer, &downloadOptions)
		if err != nil {
			log.Fatalf("Failed to download file: %v", err)
		}

		// Convert the buffer to a string and parse it as CSV
		csvReader := csv.NewReader(strings.NewReader(string(buffer)))

		// Read and parse the CSV data
		records, err := csvReader.ReadAll()
		if err != nil {
			log.Fatalf("Failed to parse CSV data: %v", err)
		}

		// Print the CSV records
		for _, record := range records {
			process(record)
		}**/

	/*	var fileURL file.FileURL
		if config.Azure.DirectoryPath != "" {
			dirURL := shareURL.NewDirectoryURL(config.Azure.DirectoryPath)
			fileURL = dirURL.NewFileURL(config.Azure.FileName)
		} else {
			fileURL = shareURL.NewRootDirectoryURL().NewFileURL(config.Azure.FileName)
		}

		// Download the file
		fileResp, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
		if err != nil {
			return fmt.Errorf("failed to download file from Azure: %w", err)
		}

		// Process the file as a CSV
		reader := csv.NewReader(fileResp.Body(azfile.RetryReaderOptions{}))

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to read CSV data: %w", err)
			}
			process(record)
		}*/

	return nil
}

// Send records to Kafka with retries and backoff
func sendToKafka(broker string, topic string, recordsChan <-chan Message, errorChan chan<- error, done chan struct{}, wg *sync.WaitGroup, maxRetries uint64, retryInterval time.Duration) {
	defer wg.Done()

	logrus.Info("Kafka initialization")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	logrus.Info("Kafka initialization failed")

	defer writer.Close()

	for record := range recordsChan {
		select {
		case <-done:
			logrus.Info("Kafka sender received shutdown signal")
			return
		default:
			msg := kafka.Message{
				Key:   []byte(record.Key), // Use the first field as the key
				Value: record.Payload,
			}

			operation := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), retryInterval)
				defer cancel()
				return writer.WriteMessages(ctx, msg)
			}

			backOff := backoff.WithMaxRetries(backoff.NewConstantBackOff(retryInterval), maxRetries)
			err := backoff.Retry(operation, backOff)
			if err != nil {
				errorChan <- fmt.Errorf("failed to write message to Kafka after retries: %w", err)
				return
			}
		}
	}
}
