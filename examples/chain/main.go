package main

import (
	"fmt"

	"github.com/andmart/caravana"
)

func main() {

	input := make(chan int)
	stage1 := make(chan int)
	output := make(chan int)

	multiplyTask := func(v int) (*int, bool, error) {
		return new(v * 2), false, nil
	}

	addTask := func(v int) (*int, bool, error) {
		return new(v + 10), false, nil
	}

	multiplyHolder := caravana.NewTaskHolder(
		input,
		multiplyTask,
		caravana.WithOutput[int, int](stage1),
	)

	addHolder := caravana.NewTaskHolder(
		stage1,
		addTask,
		caravana.WithOutput[int, int](output),
	)

	multiplyHolder.Start()
	addHolder.Start()

	go func() {
		input <- 5
	}()

	result := <-output

	fmt.Println(result) // 20
}
