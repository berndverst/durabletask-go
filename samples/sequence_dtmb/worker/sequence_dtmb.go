package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/azure/durabletaskservice"
	"github.com/microsoft/durabletask-go/task"
	"go.uber.org/zap"
)

func main() {
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(ActivitySequenceOrchestrator)
	r.AddActivity(SayHelloActivity)

	// Init the client
	ctx := context.Background()
	worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	// Start the worker
	err = worker.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start the worker: %v", err)
	}
	<-ctx.Done()
	defer worker.Shutdown(ctx)
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, r *task.TaskRegistry) (task.TaskHubWorker, error) {
	zaplogger, _ := zap.NewProduction()
	defer zaplogger.Sync() // flushes buffer, if any
	logger := zaplogger.Sugar()

	// Create an executor
	executor := task.NewTaskExecutor(r)

	// Instantiate the Azure credential for the backend
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	// Create a new backend
	options := &durabletaskservice.DurableTaskServiceBackendOptions{
		// Endpoint: "localhost:5147",
		Endpoint:        "bernd01.whitecoast-aac30fd6.eastus.azurecontainerapps.io:443",
		AzureCredential: cred,
		DisableAuth:     true,
		// Insecure:        true,
	}
	be, err := durabletaskservice.NewDurableTaskServiceBackend(ctx, options, logger)
	if err != nil {
		return nil, err
	}

	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := task.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger, r)

	return taskHubWorker, nil
}

// ActivitySequenceOrchestrator makes three activity calls in sequence and results the results
// as an array.
func ActivitySequenceOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Tokyo")).Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("London")).Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Seattle")).Await(&helloSeattle); err != nil {
		return nil, err
	}
	var helloVancouver string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Vancouver")).Await(&helloVancouver); err != nil {
		return nil, err
	}
	var helloShanghai string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Shanghai")).Await(&helloShanghai); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle, helloVancouver, helloShanghai}, nil
}

// SayHelloActivity can be called by an orchestrator function and will return a friendly greeting.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
