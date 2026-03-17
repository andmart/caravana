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

	in := make(chan int)
	mid := make(chan int)

	const total = 5
	var done atomic.Uint64

	var wg sync.WaitGroup
	wg.Add(1)

	// -----------------------------
	// Stage 1: multiply by 2
	// -----------------------------
	stage1 := caravana.NewTaskHolder(
		in,

		func(v int) (*int, bool, error) {
			return new(v * 2), false, nil
		},
		caravana.WithOutput[int, int](mid),
	)

	// -----------------------------
	// Stage 2: add 1
	// -----------------------------
	var stage2 *caravana.TaskHolder[int, int]
	stage2 = caravana.NewTaskHolder(
		mid,
		func(v int) (*int, bool, error) {
			fmt.Println(v + 1)
			return nil, false, nil
		},
		caravana.WithOnEvent(func(e caravana.Event[int, int]) {

			if e.Type == caravana.Processed {

				n := done.Add(1)

				if n == total {
					stage2.Stop()
					stage1.Stop()
					wg.Done()
				}
			}
		}),
	)

	// start pipeline
	stage1.Start()
	stage2.Start()

	// send data
	for i := 1; i <= total; i++ {
		in <- i
	}

	// wait until pipeline completes
	wg.Wait()
}
```

## ✨ Features

- ⚡ Concurrent workers per stage
- 🔁 Retry support per task
- 🔗 Pipeline composition via channels
- 📡 Event-driven observability
- 🧠 Fully generic (`TaskHolder[P, T]`)
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
- optionally emits to an output channel
- triggers events

```
input → TaskHolder → TaskHolder → TaskHolder
```

---

## ⚙️ Options

`TaskHolder` behavior is configured using functional options.

### WithOutput

```go
WithOutput(chan T)
```

Defines the output channel.

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

More in examples.

---

## 📄 License

MIT