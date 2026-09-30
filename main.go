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

// Parte 0: Estruturas de Dados e Funções Auxiliares

// Event representa a estrutura de um único log em formato JSON.
type Event struct {
	EventType string `json:"event_type"`
	Region    string `json:"region"`
}

// Report armazena os dados agregados do processamento dos logs.
// O Mutex é crucial para garantir o acesso seguro em um ambiente concorrente.
type Report struct {
	TotalEvents    int
	TotalErrors    int
	EventsByType   map[string]int
	EventsByRegion map[string]int
	mutex          sync.Mutex
}

// NewReport inicializa e retorna um ponteiro para uma nova estrutura Report.
func NewReport() *Report {
	return &Report{
		EventsByType:   make(map[string]int),
		EventsByRegion: make(map[string]int),
	}
}

// --- Métodos do Report (API de agregação) ---

// AddEvent adiciona um evento ao relatório. Esta versão NÃO é thread-safe.
func (r *Report) AddEvent(event Event) {
	r.TotalEvents++
	r.EventsByType[event.EventType]++
	r.EventsByRegion[event.Region]++
}

// AddError adiciona um erro de processamento ao relatório. NÃO é thread-safe.
func (r *Report) AddError() {
	r.TotalErrors++
}

// AddEventSafe adiciona um evento ao relatório de forma segura para concorrência.
func (r *Report) AddEventSafe(event Event) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	// Chama a lógica não-segura interna
	r.AddEvent(event)
}

// AddErrorSafe adiciona um erro ao relatório de forma segura para concorrência.
func (r *Report) AddErrorSafe() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	// Chama a lógica não-segura interna
	r.AddError()
}

// --- Fim dos Métodos do Report ---

// GenerateMockFiles cria arquivos de log JSON para serem processados.
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

// processFile é uma função auxiliar que processa um único arquivo de log.
func processFile(
	filename string,
	addEventFunc func(Event),
	addErrorFunc func(error),
) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Erro ao abrir arquivo %s: %v", filename, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		line := scanner.Bytes()
		// Ignora linhas vazias
		if len(line) == 0 {
			continue
		}

		if err := json.Unmarshal(line, &event); err != nil {
			addErrorFunc(err) // Chama a callback de erro
			continue
		}
		addEventFunc(event) // Chama a callback de sucesso
	}
}

// --- Fim da Parte 0 ---

// Parte 1: Implementação Síncrona
func ProcessSequential(files []string) *Report {
	report := NewReport()

	// Define as callbacks para a versão síncrona
	addEvent := func(e Event) { report.AddEvent(e) }
	addError := func(err error) { report.AddError() }

	for _, file := range files {
		processFile(file, addEvent, addError)
	}

	return report
}

// Parte 2: Concorrência Ingênua
func ProcessConcurrentNaive(files []string) *Report {
	report := NewReport()
	var wg sync.WaitGroup

	// Define as callbacks (não-seguras)
	addEvent := func(e Event) { report.AddEvent(e) }
	addError := func(err error) { report.AddError() }

	for _, file := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			processFile(filename, addEvent, addError)
		}(file)
	}

	wg.Wait()
	return report
}

// Parte 3: Correção com sync.Mutex
func ProcessConcurrentMutex(files []string) *Report {
	report := NewReport()
	var wg sync.WaitGroup

	// Define as callbacks (seguras)
	addEventSafe := func(e Event) { report.AddEventSafe(e) }
	addErrorSafe := func(err error) { report.AddErrorSafe() }

	for _, file := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			processFile(filename, addEventSafe, addErrorSafe)
		}(file)
	}

	wg.Wait()
	return report
}

// Estrutura para encapsular o resultado do processamento de uma linha
type ProcessResult struct {
	Event Event
	Err   error
}

// Parte 4: Padrão Pipeline
func ProcessPipeline(files []string, numWorkers int) *Report {
	jobs := make(chan string, len(files))
	// O canal de resultados agora carrega a struct ProcessResult
	results := make(chan ProcessResult, 1000)
	var wgWorkers sync.WaitGroup

	// 1. Inicia os workers
	for w := 0; w < numWorkers; w++ {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			for filename := range jobs {
				file, err := os.Open(filename)
				if err != nil {
					// Não podemos enviar o erro do Open() para o results
					// pois o agregador não saberá quando parar.
					// Em um sistema real, isso iria para um log.
					log.Printf("Erro fatal ao abrir %s: %v", filename, err)
					continue
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Bytes()
					if len(line) == 0 {
						continue
					}

					var event Event
					if err := json.Unmarshal(line, &event); err != nil {
						// CORREÇÃO: Envia o erro pelo canal
						results <- ProcessResult{Err: err}
					} else {
						// Envia o evento válido pelo canal
						results <- ProcessResult{Event: event, Err: nil}
					}
				}
				file.Close()
			}
		}()
	}

	// 2. Envia os trabalhos para o canal de jobs
	for _, file := range files {
		jobs <- file
	}
	close(jobs) // Fecha o canal de jobs

	// 3. Goroutine de coordenação para fechar o canal 'results'
	// SÓ APÓS todos os workers terminarem.
	go func() {
		wgWorkers.Wait()
		close(results)
	}()

	// 4. Agrega os resultados (Fan-In)
	report := NewReport()
	for res := range results {
		if res.Err != nil {
			// CORREÇÃO: Conta o erro
			report.AddError()
		} else {
			// Conta o evento (usa AddEvent, sem mutex, pois é single-threaded)
			report.AddEvent(res.Event)
		}
	}

	return report
}

// Parte 5: Função main (Execução e Benchmark)
func main() {
	const (
		LogDir        = "./logs"
		NumFiles      = 100 // Aumentado para ver melhor a diferença
		EventsPerFile = 1000
		NumWorkers    = 8 // Número de workers para o pool
	)

	// Setup: Gera novos arquivos de log
	fmt.Println("Gerando arquivos de log de exemplo...")
	if err := os.RemoveAll(LogDir); err != nil {
		log.Fatalf("Falha ao limpar diretório de logs: %v", err)
	}
	if err := GenerateMockFiles(LogDir, NumFiles, EventsPerFile); err != nil {
		log.Fatalf("Falha ao gerar arquivos de mock: %v", err)
	}
	fmt.Printf("%d arquivos gerados com %d eventos cada.\n\n", NumFiles, EventsPerFile)

	files, err := filepath.Glob(filepath.Join(LogDir, "*.json"))
	if err != nil {
		log.Fatalf("Falha ao listar arquivos de log: %v", err)
	}

	// --- Benchmarking ---

	// 1. Sequencial
	fmt.Print("[Sequential] \t\t")
	start := time.Now()
	reportSeq := ProcessSequential(files)
	duration := time.Since(start)
	fmt.Printf("Tempo: %v \tEventos: %d \tErros: %d\n", duration, reportSeq.TotalEvents, reportSeq.TotalErrors)

	// 2. Concorrente Ingênua
	//fmt.Print("[Concurrent Naive]\t")
	//start = time.Now()
	//reportNaive := ProcessConcurrentNaive(files)
	//duration = time.Since(start)
	//fmt.Printf("Tempo: %v \tEventos: %d (Incorreto!) \tErros: %d (Incorreto!)\n", duration, reportNaive.TotalEvents, reportNaive.TotalErrors)

	// 3. Concorrente com Mutex
	fmt.Print("[Concurrent Mutex]\t")
	start = time.Now()
	reportMutex := ProcessConcurrentMutex(files)
	duration = time.Since(start)
	fmt.Printf("Tempo: %v \tEventos: %d \tErros: %d\n", duration, reportMutex.TotalEvents, reportMutex.TotalErrors)

	// 4. Pipeline com Worker Pool
	fmt.Print("[Pipeline] \t\t")
	start = time.Now()
	reportPipeline := ProcessPipeline(files, NumWorkers)
	duration = time.Since(start)
	fmt.Printf("Tempo: %v \tEventos: %d \tErros: %d\n", duration, reportPipeline.TotalEvents, reportPipeline.TotalErrors)

	fmt.Println("\n--- Verificação de Corretude ---")
	if reportSeq.TotalEvents == reportMutex.TotalEvents &&
		reportSeq.TotalErrors == reportMutex.TotalErrors &&
		reportSeq.TotalEvents == reportPipeline.TotalEvents &&
		reportSeq.TotalErrors == reportPipeline.TotalErrors {
		fmt.Println("✅ SUCESSO: Partes 1, 3, e 4 produziram resultados idênticos.")
	} else {
		fmt.Println("❌ FALHA: Os resultados não são idênticos.")
		fmt.Printf("  Seq:     Eventos=%d, Erros=%d\n", reportSeq.TotalEvents, reportSeq.TotalErrors)
		fmt.Printf("  Mutex:   Eventos=%d, Erros=%d\n", reportMutex.TotalEvents, reportMutex.TotalErrors)
		fmt.Printf("  Pipeline: Eventos=%d, Erros=%d\n", reportPipeline.TotalEvents, reportPipeline.TotalErrors)
	}

	fmt.Println("\nExecute com 'go run -race .' para ver a falha na Parte 2.")
}
