package tests

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/tests/mocks"
	"github.com/stretchr/testify/assert"
)

func Test_TaskHubWorkerStartsDependencies(t *testing.T) {
	ctx := context.Background()

	be := mocks.NewBackend(t)
	orchWorker := mocks.NewTaskWorker(t)
	actWorker := mocks.NewTaskWorker(t)

	be.EXPECT().CreateTaskHub(ctx).Return(nil).Once()
	be.EXPECT().Start(ctx, nil).Return(nil).Once()
	orchWorker.EXPECT().Start(ctx).Return().Once()
	actWorker.EXPECT().Start(ctx).Return().Once()

	w := backend.NewTaskHubWorker(be, orchWorker, actWorker, logger, nil)
	err := w.Start(ctx)
	assert.NoError(t, err)
}

func Test_TaskHubWorkerStopsDependencies(t *testing.T) {
	ctx := context.Background()

	be := mocks.NewBackend(t)
	orchWorker := mocks.NewTaskWorker(t)
	actWorker := mocks.NewTaskWorker(t)

	be.EXPECT().Stop(ctx).Return(nil).Once()
	orchWorker.EXPECT().StopAndDrain().Return().Once()
	actWorker.EXPECT().StopAndDrain().Return().Once()

	w := backend.NewTaskHubWorker(be, orchWorker, actWorker, logger, nil)
	err := w.Shutdown(ctx)
	assert.NoError(t, err)
}
