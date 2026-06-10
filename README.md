# Caravana

**Caravana** is a Go library for building **concurrent pipelines using channels and workers**, with retry support and full observability through events.

The library is **simple, type-safe, and unopinionated**: it runs the pipeline — you decide how to observe, measure, and stop it.

---

## Example

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/andmart/caravana"
)

func main() {
	const total = 5
	var done atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(1)

	// -----------------------------
	// Stage 1: multiply by 2
	// -----------------------------
	stage1 := caravana.NewTaskHolder(
		func(v int) (*int, bool, error) {
			r := v * 2
			return &r, false, nil
		},
	)

	// -----------------------------
	// Stage 2: add 1
	// -----------------------------
	var stage2 *caravana.TaskHolder[int, int]
	stage2 = caravana.NewTaskHolder(
		func(v int) (*int, bool, error) {
			fmt.Println(v + 1)
			return nil, false, nil
		},
		caravana.WithOnEvent(func(e caravana.Event[int, int]) {
			if e.Type == caravana.Processed {
				if done.Add(1) == total {
					stage2.Stop()
					stage1.Stop()
					wg.Done()
				}
			}
		}),
	)

	// wire and start pipeline
	c := &caravana.Caravana{}
	c.Link(stage1, stage2)
	c.Start()

	// send data
	for i := 1; i <= total; i++ {
		stage1.Send(i)
	}

	wg.Wait()
}
```

## ✨ Features

- ⚡ Concurrent workers per stage
- 🔁 Retry support per task
- 🔗 Pipeline composition via channels
- 📡 Event-driven observability
- 🧠 Fully generic (`TaskHolder[P, T]`)
- 🌿 Fan-out: one stage can emit to multiple downstream stages
- 🧼 No built-in logging or metrics

---

## 📦 Installation

```bash
go get github.com/andmart/caravana
```

---

## 🧩 Concept

A pipeline is composed of **TaskHolders**, where each one:

- reads from an input channel
- executes a task
- optionally emits to one or more output channels
- triggers events

```
input → TaskHolder → TaskHolder → TaskHolder
                  ↘ TaskHolder  (fan-out)
```

---

## 🏗️ Constructors

### NewTaskHolder

```go
NewTaskHolder[P, T](task func(P) (*T, bool, error), opts ...Option[P, T]) *TaskHolder[P, T]
```

Creates a `TaskHolder` with an auto-managed input channel. Closes the input channel automatically on `Stop()`. Use this when you don't need to share the input channel externally.

---

### NewTaskHolderFrom

```go
NewTaskHolderFrom[P, T](in chan P, task func(P) (*T, bool, error), opts ...Option[P, T]) *TaskHolder[P, T]
```

Creates a `TaskHolder` from an existing channel. The caller retains ownership of the channel; it is **not** closed on `Stop()`. Use this when you manage the channel lifecycle yourself or share it across multiple holders.

---

## 🔗 Caravana (pipeline orchestrator)

`Caravana` wires and manages a set of stages as a unit.

```go
c := &caravana.Caravana{}
c.Link(stage1, stage2, stage3) // connect sequentially
c.Start()                      // start all stages
c.Stop()                       // stop all stages
```

### Link

```go
func (c *Caravana) Link(stages ...stage)
```

Connects stages in sequence: each stage's output is wired to the next stage's input. Can be called multiple times to create fan-out topologies:

```go
c.Link(stage1, stage2) // stage1 → stage2
c.Link(stage1, stage3) // stage1 → stage2 and stage1 → stage3
```

---

## ⚙️ Options

`TaskHolder` behavior is configured using functional options.

### WithOutput

```go
WithOutput(chan T)
```

Adds an output channel. Can be called multiple times to fan-out to several downstream channels.

---

### WithWorkers

```go
WithWorkers(n int)
```

Number of concurrent workers.

---

### WithInterval

```go
WithInterval(d time.Duration)
```

Delay before retry.

---

### WithOnEvent

```go
WithOnEvent(func(Event[P,T]))
```

Main observability mechanism.

---

### WithCloseChannelsOnStop

```go
WithCloseChannelsOnStop(bool)
```

Controls whether the input and output channels are closed when `Stop()` is called. Defaults to `true` for `NewTaskHolder` and `false` for `NewTaskHolderFrom`.

---

## 📡 Events

```go
const (
    Received EventType = iota
    Processed
    Emitted
    Retry
    Error
)
```

---

## 🧠 Receiving Events

```go
caravana.WithOnEvent(func(e caravana.Event[int,int]) {
    switch e.Type {
    case caravana.Received:
        fmt.Println("received", *e.In)
    case caravana.Processed:
        fmt.Println("processed")
    case caravana.Emitted:
        fmt.Println("emitted", *e.Out)
    case caravana.Error:
        fmt.Println("error:", e.Err)
    }
})
```

`Event` also implements `String()` for convenient logging:

```go
fmt.Println(e.String())
// Event{Stage: stage1, Type: Processed, In: 42, Out: <nil>, Err: <nil>, IsRetry: false}
```

More in examples.

---

## 📄 License

MIT
