package task

import (
	"context"

	"github.com/microsoft/durabletask-go/backend"
)

type TaskHubWorker interface {
	// Start starts the backend and the configured internal workers.
	Start(context.Context) error

	// Shutdown stops the backend and all internal workers.
	Shutdown(context.Context) error
}

type taskHubWorker struct {
	backend             backend.Backend
	orchestrationWorker backend.TaskWorker
	activityWorker      backend.TaskWorker
	logger              backend.Logger
	taskRegistry        *TaskRegistry
}

func NewTaskHubWorker(be backend.Backend, orchestrationWorker backend.TaskWorker, activityWorker backend.TaskWorker, logger backend.Logger, taskRegistry *TaskRegistry) TaskHubWorker {
	return &taskHubWorker{
		backend:             be,
		orchestrationWorker: orchestrationWorker,
		activityWorker:      activityWorker,
		logger:              logger,
		taskRegistry:        taskRegistry,
	}
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	// TODO: Check for already started worker
	if err := w.backend.CreateTaskHub(ctx); err != nil && err != backend.ErrTaskHubExists {
		return err
	}

	var orchestrators []string = nil
	var activities []string = nil

	// If a task registry reference was provided we can get the orchestrator and activity names
	if w.taskRegistry != nil {
		orchestrators = w.taskRegistry.GetOrchestratorNames()
		activities = w.taskRegistry.GetActivityNames()
	}

	if err := w.backend.Start(ctx, orchestrators, activities); err != nil {
		return err
	}
	w.logger.Infof("worker started with backend %v", w.backend)

	w.orchestrationWorker.Start(ctx)
	w.activityWorker.Start(ctx)
	return nil
}

func (w *taskHubWorker) Shutdown(ctx context.Context) error {
	w.logger.Info("backend stopping...")
	if err := w.backend.Stop(ctx); err != nil {
		return err
	}

	w.logger.Info("workers stopping and draining...")
	w.orchestrationWorker.StopAndDrain()
	w.activityWorker.StopAndDrain()
	return nil
}
