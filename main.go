package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func main() {
	//GenerateMockFiles("./logs", 11, 5000)

	files := []string{
		"./logs/log_000.json",
		"./logs/log_001.json",
		"./logs/log_002.json",
		"./logs/log_003.json",
		"./logs/log_004.json",
		"./logs/log_005.json",
		"./logs/log_006.json",
		"./logs/log_007.json",
		"./logs/log_008.json",
		"./logs/log_009.json",
		"./logs/log_010.json",
	}
	// Exemplo de Procesamento Sequencial
	//fmt.Println("Inicio da Leitura Sequencial: ")
	//start := time.Now()
	//report := ProcessSequential(files)
	//elapsed := time.Since(start)
	//fmt.Println("Fim da Leitura do Processamento Sequencial : ", elapsed)
	//fmt.Println(report.Errors)

	//fmt.Println("Inicio da Leitura Concorrente: ")
	//start := time.Now()
	//report := ProcessConcurrentNaive(files)
	//elapsed := time.Since(start)
	//fmt.Println("Fim da Leitura do Processamento Concorrente : ", elapsed)
	//fmt.Println(report.Errors)

	fmt.Println("Inicio da Leitura Concorrente com Mutex (A Solução de Memória Compartilhada): ")
	start := time.Now()
	report := ProcessConcurrentMutex(files)
	elapsed := time.Since(start)
	fmt.Println("Fim da Leitura do Processamento Concorrente  com Mutex (A Solução de Memória Compartilhada): ", elapsed)
	fmt.Println(report.Errors)

}

type Event struct {
	EventType string `json:"event_type"`
	Region    string `json:"region"`
}

func GenerateMockFiles(dir string, numFiles, eventsPerFile int) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	eventTypes := []string{"click", "view", "purchase", "login"}
	regions := []string{"us-east-1", "eu-west-1", "ap-southeast-2", "sa-east-1"}

	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(dir, fmt.Sprintf("log_%03d.json", i))
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}

		for j := 0; j < eventsPerFile; j++ {
			var line string
			if j%50 == 0 && j > 0 {
				line = "this is not valid json\n"
			} else {
				event := Event{
					EventType: eventTypes[(i+j)%len(eventTypes)],
					Region:    regions[j%len(regions)],
				}
				data, _ := json.Marshal(event)
				line = string(data) + "\n"
			}

			if _, err := file.WriteString(line); err != nil {
				log.Printf("Erro ao escrever linha no arquivo %s: %v", filePath, err)
			}
		}
		file.Close()
	}
	return nil
}

type Report struct {
	Events []Event
	Errors int
	mu     sync.Mutex
}

func NewReport() *Report {
	return &Report{
		Events: make([]Event, 0),
		Errors: 0,
	}
}

func (r *Report) AddEvent(event Event) {
	r.Events = append(r.Events, event)
}
func (r *Report) AddEventSafe(event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, event)
}
func (r *Report) AddError() {
	r.Errors++
}
func (r *Report) AddErrorSafe() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Errors++
}
func ProcessSequential(files []string) *Report {

	report := NewReport()

	for _, file := range files {
		fileHandle, err := os.Open(file)

		if err != nil {
			println("Erro de Arquivo : ", file)
			report.AddError()
			continue
		}

		scanner := bufio.NewScanner(fileHandle)

		for scanner.Scan() {
			line := scanner.Text()

			var event Event

			err := json.Unmarshal([]byte(line), &event)

			if err != nil {
				report.AddError()
				continue
			}

			report.AddEvent(event)
		}

		fileHandle.Close()
	}

	return report
}
func ProcessConcurrentNaive(files []string) *Report {
	report := NewReport()
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)

		go func(filename string) {
			defer wg.Done()
			fileHandle, err := os.Open(file)

			if err != nil {
				println("Erro de Arquivo : ", file)
				report.AddError()
				return
			}

			scanner := bufio.NewScanner(fileHandle)

			for scanner.Scan() {
				line := scanner.Text()

				var event Event

				err := json.Unmarshal([]byte(line), &event)

				if err != nil {
					report.AddError()
					continue
				}

				report.AddEvent(event)
			}

			defer fileHandle.Close()
		}(file)

	}
	wg.Wait()
	return report
}

func ProcessConcurrentMutex(files []string) *Report {
	report := NewReport()
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)

		go func(filename string) {
			defer wg.Done()
			fileHandle, err := os.Open(file)

			if err != nil {
				println("Erro de Arquivo : ", file)
				report.AddErrorSafe()
				return
			}
			defer fileHandle.Close()

			scanner := bufio.NewScanner(fileHandle)

			for scanner.Scan() {
				line := scanner.Text()

				var event Event

				err := json.Unmarshal([]byte(line), &event)

				if err != nil {
					report.AddErrorSafe()
					continue
				}

				report.AddEventSafe(event)
			}

		}(file)

	}
	wg.Wait()
	return report
}
