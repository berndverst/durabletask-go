package main

import (
	"context"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/azure/durabletaskservice"
	"go.uber.org/zap"
)

func main() {
	// Init the client
	zaplogger, _ := zap.NewProduction()
	defer zaplogger.Sync() // flushes buffer, if any
	logger := zaplogger.Sugar()

	ctx := context.Background()
	client, err := Init(ctx, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize the client: %v", err)
	}

	// read input parameter from command line
	var count int
	if len(os.Args) > 1 {
		count, err = strconv.Atoi(os.Args[1])
		if err != nil {
			logger.Fatalf("Failed to parse input parameter: %v", err)
			return
		}
	} else {
		count = 1
	}

	launchWorkflowFunc := func(done chan<- bool, results chan<- time.Duration) {
		newCtx := context.WithoutCancel(ctx)
		// Start a new orchestration
		now := time.Now()

		id, scheduleErr := client.ScheduleNewOrchestration(newCtx, "ActivitySequenceOrchestrator")

		if scheduleErr != nil {
			logger.Fatalf("Failed to schedule new orchestration: %v", scheduleErr)
		}

		// Wait for the orchestration to complete
		_, waitErr := client.WaitForOrchestrationCompletion(newCtx, id)
		if waitErr != nil {
			logger.Fatalf("Failed to wait for orchestration to complete: %v", waitErr)
		}
		duration := time.Since(now)
		results <- duration
		done <- true

		// Print the results
		// metadataEnc, marshalErr := json.MarshalIndent(metadata, "", "  ")
		// if err != nil {
		// 	logger.Fatalf("Failed to encode result to JSON: %v", marshalErr)
		// }
		// logger.Debugf("Orchestration completed: %v", string(metadataEnc))
	}

	done := make(chan bool, count)
	results := make(chan time.Duration, count)

	overallStart := time.Now()
	for i := 0; i < count; i++ {
		go launchWorkflowFunc(done, results)
	}
	for i := 0; i < count; i++ {
		<-done
	}
	overallDuration := time.Since(overallStart)
	close(results)
	logger.Infof("All %d orchestrations completed in %v", count, overallDuration)

	var durations []time.Duration
	for duration := range results {
		durations = append(durations, duration)
	}

	var total time.Duration
	for _, d := range durations {
		total += d
	}
	average := total / time.Duration(count)
	logger.Infof("Average time taken for %d orchestrations: %v", count, average)

	// Compute percentiles.
	percentiles := []int{10, 50, 90}
	percentileValues := computePercentiles(durations, percentiles)
	// Print the results.
	logger.Infof("10th percentile: %v\n", percentileValues[0])
	logger.Infof("50th percentile: %v\n", percentileValues[1])
	logger.Infof("90th percentile: %v\n", percentileValues[2])
}

func computePercentiles(durations []time.Duration, percentiles []int) []time.Duration {
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	results := make([]time.Duration, len(percentiles))
	for i, percentile := range percentiles {
		index := int(float64(len(durations)) * float64(percentile) / 100.0)
		if index >= len(durations) {
			index = len(durations) - 1
		}
		results[i] = durations[index]
	}
	return results
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, logger backend.Logger) (backend.TaskHubClient, error) {
	// Instantiate the Azure credential for the backend
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	// Create a new backend
	options := []durabletaskservice.DurableTaskServiceBackendConfigurationOption{
		durabletaskservice.WithCredential(cred),
		durabletaskservice.WithDisableAuth(),
	}

	endpoint := os.Getenv("DURABLE_TASK_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:5147"
		options = append(options, durabletaskservice.WithInsecureMode())
	}
	taskHubName := "default"

	be, err := durabletaskservice.NewDurableTaskServiceBackend(ctx, logger, endpoint, taskHubName, options...)
	if err != nil {
		return nil, err
	}

	// Get the client to the backend
	taskHubClient := backend.NewTaskHubClient(be)

	return taskHubClient, nil
}
