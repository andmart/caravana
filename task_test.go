package caravana

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTaskHolder_Process verifies that a TaskHolder consumes input values,
// executes the configured task, and emits the resulting value to the
// output channel.
//
// The test sends a value to the input channel and checks that the task
// processes it correctly and forwards the expected result.
func TestTaskHolder_Process(t *testing.T) {

	in := make(chan int)
	out := make(chan int)

	task := func(v int) (*int, bool, error) {
		return new(v * 2), false, nil
	}

	holder := NewTaskHolder(
		in,
		task,
		WithOutput[int, int](out),
		WithWorkers[int, int](1),
	)

	go holder.Start()

	in <- 10

	select {
	case v := <-out:
		if v != 20 {
			t.Fatalf("expected 20 got %d", v)
		}

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for output")
	}
}

// TestTaskHolder_Retry verifies that a task requesting retry is executed
// repeatedly until it returns retry=false.
//
// The test simulates a task that requires multiple attempts before
// producing a result and confirms that the task is executed the expected
// number of times.
func TestTaskHolder_Retry(t *testing.T) {

	in := make(chan int)
	out := make(chan int)

	count := 0

	task := func(v int) (*int, bool, error) {

		count++

		if count < 3 {
			return nil, true, nil
		}

		return new(v), false, nil
	}

	holder := NewTaskHolder(
		in,
		task,
		WithOutput[int, int](out),
		WithWorkers[int, int](1),
		WithInterval[int, int](10*time.Millisecond),
	)

	go holder.Start()

	in <- 1

	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if count != 3 {
		t.Fatalf("expected 3 executions got %d", count)
	}
}

// TestTaskHolder_Workers verifies that multiple workers process tasks
// concurrently.
//
// The test submits multiple input values and ensures that all tasks are
// processed and results are emitted when multiple workers are configured.
func TestTaskHolder_Workers(t *testing.T) {

	in := make(chan int, 10)
	out := make(chan int, 10)

	task := func(v int) (*int, bool, error) {
		time.Sleep(10 * time.Millisecond)
		return new(v), false, nil
	}

	holder := NewTaskHolder(
		in,
		task,
		WithOutput[int, int](out),
		WithWorkers[int, int](4),
	)

	go holder.Start()

	for i := 0; i < 10; i++ {
		in <- i
	}

	count := 0

	timeout := time.After(time.Second)

	for count < 10 {
		select {
		case <-out:
			count++

		case <-timeout:
			t.Fatal("timeout waiting for workers")
		}
	}
}

// TestTaskHolder_DefaultWorker verifies that a TaskHolder runs correctly
// when no worker count is explicitly configured.
//
// When WithWorkers is not provided, the TaskHolder should default to a
// single worker. The test ensures that the task is executed and the
// resulting value is emitted to the output channel.
func TestTaskHolder_DefaultWorker(t *testing.T) {

	in := make(chan int)
	out := make(chan int)

	task := func(v int) (*int, bool, error) {
		return new(v + 1), false, nil
	}

	holder := NewTaskHolder(
		in,
		task,
		WithOutput[int, int](out),
	)

	go holder.Start()

	in <- 10

	select {
	case result := <-out:
		if result != 11 {
			t.Fatalf("expected 11, got %d", result)
		}

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}

// TestTaskHolder_Stop verifies that calling Stop cancels all workers and
// prevents further task execution.
func TestTaskHolder_Stop(t *testing.T) {

	in := make(chan int, 10)
	out := make(chan int, 10)

	executions := 0

	task := func(v int) (*int, bool, error) {
		executions++
		return new(v), false, nil
	}

	holder := NewTaskHolder(in, task, WithOutput[int, int](out))

	holder.Start()

	in <- 1
	in <- 2

	time.Sleep(50 * time.Millisecond)

	holder.Stop()

	// Enviar mais dados após Stop
	in <- 3
	in <- 4

	time.Sleep(50 * time.Millisecond)

	if executions != 2 {
		t.Fatalf("tasks executed after stop: %d", executions)
	}
}

// TestEventReceived verifies that the Received event is triggered
// when a TaskHolder consumes an item from the input channel.
//
// Expected behavior:
//   - When an item is read from the input channel
//   - The Received event must be emitted exactly once.
func TestEventReceived(t *testing.T) {

	in := make(chan int, 1)

	var received atomic.Uint64
	var wg sync.WaitGroup

	// The WaitGroup ensures the test waits until the event fires.
	wg.Add(1)

	holder := NewTaskHolder(
		in,

		// Simple task that returns the input value unchanged.
		func(v int) (*int, bool, error) {
			return &v, false, nil
		},

		// Event callback increments the counter when a Received event occurs.
		WithOnEvent(func(e Event[int, int]) {

			if e.Type == Received {
				received.Add(1)
				wg.Done()
			}
		}),
	)

	holder.Start()

	// Send a single item to trigger the Received event.
	in <- 42

	wg.Wait()

	if received.Load() != 1 {
		t.Fatalf("expected 1 received event, got %d", received.Load())
	}

	holder.Stop()
}

// TestEventEmitted verifies that the Emitted event is triggered
// when a TaskHolder sends a processed result to the output channel.
//
// Expected behavior:
//   - A task processes an input item
//   - The output value is sent to the output channel
//   - The Emitted event is fired exactly once.
func TestEventEmitted(t *testing.T) {

	in := make(chan int, 1)
	out := make(chan int, 1)

	var emitted atomic.Uint64
	var wg sync.WaitGroup

	wg.Add(1)

	holder := NewTaskHolder(
		in,
		func(v int) (*int, bool, error) {
			return &v, false, nil
		},
		WithOutput[int, int](out),
		WithOnEvent(func(e Event[int, int]) {
			if e.Type == Emitted {
				emitted.Add(1)
				wg.Done()
			}
		}),
	)

	holder.Start()

	// Send a single item that should be emitted after processing.
	in <- 10

	wg.Wait()

	if emitted.Load() != 1 {
		t.Fatalf("expected 1 emitted event, got %d", emitted.Load())
	}

	holder.Stop()
}

// TestEventRetry verifies that the Retry event is triggered
// when a task requests re-execution.
//
// Expected behavior:
//   - The task returns retry=true on the first execution
//   - The Retry event must be emitted
//   - The task is executed again.
func TestEventRetry(t *testing.T) {

	in := make(chan int, 1)

	var retries atomic.Uint64
	var wg sync.WaitGroup

	wg.Add(1)

	first := true

	holder := NewTaskHolder(
		in,
		// First execution triggers a retry.
		func(v int) (*int, bool, error) {

			if first {
				first = false
				return nil, true, nil
			}

			return &v, false, nil
		},

		WithOnEvent(func(e Event[int, int]) {
			if e.Type == Retry {
				retries.Add(1)
				wg.Done()
			}
		}),
	)

	holder.Start()

	in <- 1

	wg.Wait()

	if retries.Load() != 1 {
		t.Fatalf("expected 1 retry event, got %d", retries.Load())
	}

	holder.Stop()
}

// TestEventError verifies that the Error event is triggered
// when a task returns an error.
//
// Expected behavior:
//   - The task returns an error
//   - The Error event is emitted exactly once.
func TestEventError(t *testing.T) {

	in := make(chan int, 1)

	var errors atomic.Uint64
	var wg sync.WaitGroup

	wg.Add(1)

	holder := NewTaskHolder(
		in,
		// Task always fails.
		func(v int) (*int, bool, error) {
			return nil, false, fmt.Errorf("test error")
		},

		WithOnEvent(func(e Event[int, int]) {

			if e.Type == Error {
				errors.Add(1)
				wg.Done()
			}
		}),
	)

	holder.Start()

	in <- 5

	wg.Wait()

	if errors.Load() != 1 {
		t.Fatalf("expected 1 error event, got %d", errors.Load())
	}

	holder.Stop()
}

// TestEventIncludesStage verifies that the Event contains
// the correct TaskHolder stage name.
func TestEventIncludesStage(t *testing.T) {

	in := make(chan int, 1)

	const stageName = "MyStage"

	var wg sync.WaitGroup
	wg.Add(1)

	holder := NewTaskHolder(
		in,

		func(v int) (*int, bool, error) {
			return &v, false, nil
		},

		WithName[int, int](stageName),

		WithWorkers[int, int](1),

		WithOnEvent(func(e Event[int, int]) {
			if e.Type == Processed {
				if e.Stage != stageName {
					t.Fatalf("expected stage '%s', got '%s'", stageName, e.Stage)
				}
				wg.Done()
			}
		}),
	)

	holder.Start()

	in <- 42

	wg.Wait()

	holder.Stop()
}

func TestTaskHolder_New_ShouldCloseChannelsOnStopByDefault(t *testing.T) {
	// --------------------------------------------------
	// GIVEN
	// --------------------------------------------------
	// New() deve configurar closeOnStop = true por padrão.
	// Isso garante que pipelines simples não vazem goroutines.
	th := New[int, int](func(p int) (*int, bool, error) {
		return &p, false, nil
	})

	// --------------------------------------------------
	// WHEN
	// --------------------------------------------------
	th.Stop()

	// --------------------------------------------------
	// THEN
	// --------------------------------------------------
	// Esperamos que os channels tenham sido fechados.
	// Leitura deve retornar ok=false imediatamente.
	select {
	case _, ok := <-th.in:
		if ok {
			t.Fatalf("expected in channel to be closed")
		}
	default:
		t.Fatalf("expected in channel to be closed and readable")
	}
}

func TestTaskHolder_NewTaskHolder_ShouldNotCloseChannelsByDefault(t *testing.T) {
	// --------------------------------------------------
	// GIVEN
	// --------------------------------------------------
	// Quando o usuário fornece o channel,
	// a lib NÃO deve assumir ownership dele.
	in := make(chan int)
	th := NewTaskHolder[int, int](in, func(p int) (*int, bool, error) {
		return &p, false, nil
	})

	// --------------------------------------------------
	// WHEN
	// --------------------------------------------------
	th.Stop()

	// --------------------------------------------------
	// THEN
	// --------------------------------------------------
	// O channel NÃO deve ser fechado.
	select {
	case _, ok := <-in:
		if !ok {
			t.Fatalf("expected in channel to remain open")
		}
	default:
		// channel aberto (sem dados) → OK
	}
}

func TestTaskHolder_WithCloseChannelsOnStop_ShouldOverrideDefault(t *testing.T) {
	// --------------------------------------------------
	// GIVEN
	// --------------------------------------------------
	// Mesmo usando New(), podemos sobrescrever o default.
	th := New[int, int](
		func(p int) (*int, bool, error) {
			return &p, false, nil
		},
		WithCloseChannelsOnStop[int, int](false),
	)

	// --------------------------------------------------
	// WHEN
	// --------------------------------------------------
	th.Stop()

	// --------------------------------------------------
	// THEN
	// Channel NÃO deve estar fechado
	select {
	case _, ok := <-th.in:
		if !ok {
			t.Fatalf("expected in channel to remain open")
		}
	default:
		//ok
	}
}

func TestTaskHolder_Stop_CloseChannels_CanCausePanicOnWriters(t *testing.T) {
	// --------------------------------------------------
	// GIVEN
	// --------------------------------------------------
	// Se closeOnStop=true, writers externos podem dar panic.
	// Esse teste documenta esse comportamento explicitamente.
	th := New[int, int](func(p int) (*int, bool, error) {
		return &p, false, nil
	})

	th.Stop()

	// --------------------------------------------------
	// WHEN / THEN
	// --------------------------------------------------
	// Escrever em channel fechado deve dar panic.
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when writing to closed channel")
		}
	}()

	th.in <- 1
}

// TestLink_FanOut verifies that calling Link multiple times with the same
// source stage correctly accumulates multiple downstream connections
// (fan-out behavior), instead of overwriting previous links.
func TestLink_FanOut(t *testing.T) {
	c := &Caravana{}

	noop := func(in int) (*int, bool, error) {
		return &in, false, nil
	}

	th1 := New[int, int](noop)
	th2 := New[int, int](noop)
	th3 := New[int, int](noop)
	th4 := New[int, int](noop)

	c.Link(th1, th2)
	c.Link(th1, th3)
	c.Link(th1, th4)

	if len(th1.out) != 3 {
		t.Fatalf("expected 3 outs, got %d", len(th1.out))
	}

	// verificar se os canais corretos estão lá
	found := map[chan int]bool{
		th2.getIn().(chan int): false,
		th3.getIn().(chan int): false,
		th4.getIn().(chan int): false,
	}

	for c, _ := range th1.out {
		if _, ok := found[c]; ok {
			found[c] = true
		}
	}

	for ch, ok := range found {
		if !ok {
			t.Fatalf("missing connection to channel %v", ch)
		}
	}
}
