package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/andmart/caravana"
)

type Logger struct{}

func (Logger) Debug(message string) {
	fmt.Println(message)
}
func (Logger) Error(message string) {
	fmt.Println(message)
}

type Image struct {
	name string
	data []byte
}

func main() {

	const total = 5

	inputChannel := make(chan string)
	urlsChannel := make(chan string)
	imagesChannel := make(chan Image)

	var wg sync.WaitGroup

	var saved atomic.Uint64

	// -----------------------------
	// FetchCats
	// -----------------------------
	fetchTask := caravana.NewTaskHolderFrom(
		inputChannel,
		func(tag string) (*string, bool, error) {

			return new(fmt.Sprintf("https://cataas.com/cat/says/%s", tag)), false, nil
		},
		caravana.WithOutput[string, string](urlsChannel),
		caravana.WithOnEvent[string, string](func(e caravana.Event[string, string]) {
			fmt.Println("FETCH", e.Type.String(), *e.In)
		}),
	)

	// -----------------------------
	// DownloadCats
	// -----------------------------
	downloadTask := caravana.NewTaskHolderFrom(
		urlsChannel,
		func(url string) (*Image, bool, error) {
			resp, err := http.Get(url)
			if err != nil {
				return nil, true, err
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, true, err
			}

			return &Image{name: path.Base(url), data: data}, false, nil
		},
		caravana.WithOutput[string, Image](imagesChannel),
		caravana.WithOnEvent[string, Image](func(e caravana.Event[string, Image]) {
			fmt.Println("DOWNLOADER", e.Type.String(), *e.In)
		}),
	)

	// -----------------------------
	// SaveCats
	// -----------------------------
	var save *caravana.TaskHolder[Image, string]
	save = caravana.NewTaskHolderFrom[Image, string](
		imagesChannel,
		func(img Image) (*string, bool, error) {
			name := fmt.Sprintf("%s.jpg", img.name)
			err := os.WriteFile(name, img.data, 0644)
			if err != nil {
				return nil, false, err
			}
			return &img.name, false, nil
		},
		caravana.WithOnEvent(func(e caravana.Event[Image, string]) {
			if e.Type == caravana.Processed {
				n := saved.Add(1)
				fmt.Println("saved:")
				if n == total {
					fmt.Println("all cats saved, stopping pipeline")
					save.Stop()
					downloadTask.Stop()
					fetchTask.Stop()
					wg.Done()
				}
			}
		}),
	)

	// -----------------------------
	// start pipeline
	// -----------------------------
	fetchTask.Start()
	downloadTask.Start()
	save.Start()
	wg.Add(1)

	// send inputChannel
	for i := 0; i < total; i++ {
		inputChannel <- fmt.Sprintf("cat%d", i)
	}

	wg.Wait()
}
