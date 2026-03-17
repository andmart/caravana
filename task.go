package caravana

import (
	"context"
	"time"
)

// A TaskHolder reads values of type P from the input channel, executes the
// configured Task, and optionally emits results of type T to the output
// channel. Errors returned by the Task are forwarded to the error channel.
//
// Task execution is performed by a configurable number of worker goroutines.
// Each worker processes items independently, allowing concurrent execution
// of tasks while preserving a simple pipeline model.
//
// If a Task requests a retry, it will be executed again after the configured
// interval before completing.
//
// Fields:
//
//	name
//	  Optional identifier used for logging or debugging.
//
//	interval
//	  Delay applied between retry attempts when a task requests retry.
//
//	task
//	  The function executed for each input value.
//
//	in
//	  Channel from which input values are consumed.
//
//	out
//	  Channel where successful task results are emitted when non-nil.
//
//	err
//	  Channel where task errors are forwarded when non-nil.
//
//	workers
//	  Number of worker goroutines processing the input stream.
type TaskHolder[P any, T any] struct {
	name     string
	interval time.Duration
	task     func(P) (*T, bool, error)
	in       chan P
	out      chan T
	workers  int
	cancel   context.CancelFunc
	onEvent  OnEvent[P, T]
}

func NewTaskHolder[P any, T any](in chan P, task func(P) (*T, bool, error), opts ...Option[P, T]) *TaskHolder[P, T] {
	th := &TaskHolder[P, T]{
		in:   in,
		task: task,
	}
	for _, opt := range opts {
		opt(th)
	}
	return th
}

func (th *TaskHolder[P, T]) handle(event EventType, p *P, t *T, err error, retry bool) {
	go func() {
		if th.onEvent != nil {
			th.onEvent(Event[P, T]{Type: event, In: p, Out: t, Err: err, IsRetry: retry})
		}
	}()
}

func (th *TaskHolder[P, T]) container(p P) {
	th.handle(Received, &p, nil, nil, false)
	for {
		result, retry, err := th.task(p)
		if err != nil {
			th.handle(Error, &p, nil, err, retry)
		}
		if result != nil && th.out != nil {
			th.handle(Emitted, &p, result, nil, retry)
			th.out <- *result
		}
		if !retry {
			th.handle(Processed, &p, result, nil, retry)
			return
		}
		th.handle(Retry, &p, result, err, retry)
		time.Sleep(th.interval)
	}
}

func (th *TaskHolder[P, T]) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-th.in:
			if !ok {
				return
			}
			th.container(data)
		}
	}
}

func (th *TaskHolder[P, T]) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	th.cancel = cancel

	if th.workers <= 0 {
		th.workers = 1
	}
	for i := 0; i < th.workers; i++ {
		go th.worker(ctx)
	}
}

func (th *TaskHolder[P, T]) Stop() {
	if th.cancel != nil {
		th.cancel()
	}
}
