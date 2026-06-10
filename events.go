package caravana

import "fmt"

type EventType int

const (
	Received EventType = iota
	Processed
	Emitted
	Retry
	Error
)

func (t EventType) String() string {
	switch t {
	case Received:
		return "received"
	case Processed:
		return "processed"
	case Emitted:
		return "emitted"
	case Retry:
		return "retry"
	case Error:
		return "error"
	}
	return "unknown"
}

type Event[P any, T any] struct {
	Stage   string
	Type    EventType
	In      *P
	Out     *T
	Err     error
	IsRetry bool
}

type OnEvent[P any, T any] func(Event[P, T])

func (e *Event[P, T]) String() string {
	return fmt.Sprintf("Event{Stage: %s, Type: %s, In: %v, Out: %v, Err: %v, IsRetry: %v}",
		e.Stage, e.Type, e.In, e.Out, e.Err, e.IsRetry)
}
