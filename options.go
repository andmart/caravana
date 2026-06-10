package caravana

import "time"

// Option represents a configuration function used to modify a TaskHolder
// during construction.
//
// Options are applied by NewTaskHolder to configure optional behavior such
// as the task name, retry interval, channels, worker count, or logger.
//
// This pattern allows the TaskHolder to be configured in a flexible and
// extensible way without requiring a large constructor with many parameters.
type Option[P any, T any] func(*TaskHolder[P, T])

func WithName[P any, T any](name string) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.name = name
	}
}

func WithInterval[P any, T any](d time.Duration) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.interval = d
	}
}

func WithOutput[P any, T any](ch chan T) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.out[ch] = true
	}
}

func WithWorkers[P any, T any](n int) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.workers = n
	}
}

func WithOnEvent[P any, T any](cb OnEvent[P, T]) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.onEvent = cb
	}
}

func WithCloseChannelsOnStop[P any, T any](shouldClose bool) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.closeOnStop = shouldClose
	}
}

func WithMaxRetries[P any, T any](n int) Option[P, T] {
	return func(th *TaskHolder[P, T]) {
		th.maxRetries = n
	}
}
