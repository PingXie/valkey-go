package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// --- Constants ---
const (
	keyPrefix        = "csmget_bench"
	NilValueString   = "(nil)"
	ErrorValueString = "<error_val>"
)

// --- Custom Enum Flag Type ---

type modeFlag string

const (
	modeDoMulti  modeFlag = "DoMulti"
	modeParallel modeFlag = "Parallel"
	modeGet      modeFlag = "Get"
)

func (m *modeFlag) String() string { return string(*m) }

func (m *modeFlag) Set(value string) error {
	switch value {
	case string(modeDoMulti), string(modeParallel), string(modeGet):
		*m = modeFlag(value)
		return nil
	default:
		return fmt.Errorf("invalid mode %q, must be one of 'DoMulti', 'MGet', or 'parallel'", value)
	}
}

// --- Latency Measurement ---

type LatencyHistogram struct {
	mu        sync.Mutex
	latencies []time.Duration
	name      string
}

func NewLatencyHistogram(name string) *LatencyHistogram {
	return &LatencyHistogram{name: name, latencies: make([]time.Duration, 0)}
}

func (h *LatencyHistogram) Add(d time.Duration) {
	h.mu.Lock()
	h.latencies = append(h.latencies, d)
	h.mu.Unlock()
}

func (h *LatencyHistogram) Print() {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Printf("\n--- Latency Report for '%s' ---\n", h.name)
	if len(h.latencies) == 0 {
		fmt.Println("No data collected.")
		return
	}
	sort.Slice(h.latencies, func(i, j int) bool { return h.latencies[i] < h.latencies[j] })
	count := len(h.latencies)
	var total time.Duration
	for _, d := range h.latencies {
		total += d
	}
	fmt.Printf("Total Requests: %d\n", count)
	if count > 0 {
		avg := total / time.Duration(count)
		p50 := h.latencies[int(float64(count-1)*0.50)]
		p90 := h.latencies[int(float64(count-1)*0.90)]
		p95 := h.latencies[int(float64(count-1)*0.95)]
		p99 := h.latencies[int(float64(count-1)*0.99)]
		max := h.latencies[count-1]
		fmt.Printf("  Avg: %v\n", avg.Round(time.Microsecond))
		fmt.Printf("  P50: %v\n", p50.Round(time.Microsecond))
		fmt.Printf("  P90: %v\n", p90.Round(time.Microsecond))
		fmt.Printf("  P95: %v\n", p95.Round(time.Microsecond))
		fmt.Printf("  P99: %v\n", p99.Round(time.Microsecond))
		fmt.Printf("  Max: %v\n", max.Round(time.Microsecond))
	}
}

// --- Data Preparation & Test Logic ---

type config struct {
	host        string
	port        string
	durationSec int
	numKeys     int
	prepKeys    int
	valueLength int
	threads     int
	verbose     bool
	mode        modeFlag
	validate    bool
	populate   bool
}

func randString(n int, seededRand *rand.Rand) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[seededRand.Intn(len(letterBytes))]
	}
	return string(b)
}

func prepareKeys(numKeys int) []string {
 	fmt.Printf("INFO: Poreparing %d keys...\n", numKeys)
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("%s:%d", keyPrefix, i)
	}
	return keys
}

func prepareData(ctx context.Context, client valkey.Client, keys []string, valueLength int) (map[string]string, error) {
	fmt.Printf("INFO: Preparing values ...\n")
	numKeys := len(keys)
	preparedKVs := make(map[string]string, numKeys)
	cmds := make(valkey.Commands, 0, numKeys)
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("%s:%d", keyPrefix, i)
		value := randString(valueLength, seededRand)
		preparedKVs[key] = value
		cmds = append(cmds, client.B().Set().Key(key).Value(value).Build())
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("%s:%d", keyPrefix, i)
		value := randString(valueLength, seededRand)
		preparedKVs[key] = value
		cmds = append(cmds, client.B().Set().Key(key).Value(value).Build())
	}
	responses := client.DoMulti(ctx, cmds...)
	for _, resp := range responses {
		if err := resp.Error(); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: Failed to SET a key during preparation: %v\n", err)
		}
	}
	fmt.Printf("INFO: Preparation complete. %d keys populated.\n", len(preparedKVs))
	return preparedKVs, nil
}

func runSingleCycle(ctx context.Context, client valkey.Client, csClient valkey.CrossSlotClient, allPreparedKeys []string, preparedData map[string]string, cfg *config, metrics *LatencyHistogram, workerID int, cycleNum int64) {
	keysForThisCycle := make([]string, cfg.numKeys)
	for i := 0; i < cfg.numKeys; i++ {
		keysForThisCycle[i] = allPreparedKeys[rand.Intn(len(allPreparedKeys))]
	}

	var actualFetchedData map[string]string
	start := time.Now()

	switch cfg.mode {
	case modeDoMulti, modeParallel:
		mgetCmds, err := csClient.BuildCrossSlotMGETs(ctx, keysForThisCycle)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[W:%d C:%d] CRITICAL: Error building MGET commands: %v\n", workerID, cycleNum, err)
			return
		}
		if cfg.mode == modeDoMulti {
			actualFetchedData = executeAndCollectWithDoMulti(cfg.verbose, ctx, client, mgetCmds, workerID, cycleNum)
		} else {
			actualFetchedData = executeAndCollectWithParallel(cfg.verbose, ctx, client, mgetCmds, workerID, cycleNum)
		}
	case modeGet:
		actualFetchedData = executeAndCollectWithMGet(cfg.verbose, ctx, client,  keysForThisCycle, workerID, cycleNum)
	}

	latency := time.Since(start)
	metrics.Add(latency)

	if cfg.validate && preparedData != nil {
		expectedKVsForCycle := make(map[string]string, len(keysForThisCycle))
		for _, key := range keysForThisCycle {
			expectedKVsForCycle[key] = preparedData[key]
		}
		verifyData(cfg.verbose, expectedKVsForCycle, actualFetchedData, workerID, cycleNum)
	}
}

// --- Main Program ---

func main() {
	cfg := &config{}
	flag.StringVar(&cfg.host, "host", "127.0.0.1", "Valkey server host")
	flag.StringVar(&cfg.port, "port", "6379", "Valkey server port")
	flag.IntVar(&cfg.durationSec, "duration", 10, "Test duration in seconds")
	flag.IntVar(&cfg.numKeys, "numkeys", 100, "Number of keys to MGET per cycle")
	flag.IntVar(&cfg.prepKeys, "prepkeys", 10000, "Number of keys to pre-populate")
	flag.IntVar(&cfg.valueLength, "valueLength", 1024, "Size of each value in bytes (default: 1024)")
	flag.IntVar(&cfg.threads, "threads", 4, "Number of concurrent threads to run test cycles")
	flag.BoolVar(&cfg.verbose, "verbose", false, "Enable verbose output")
	flag.BoolVar(&cfg.validate, "validate", false, "Perform data validation on each cycle")
	flag.BoolVar(&cfg.populate, "populate", false, "Populate Valkey with random data before running tests")
	cfg.mode = modeDoMulti
	flag.Var(&cfg.mode, "mode", "Execution mode: 'DoMulti', 'Get', or 'Parallel'")
	flag.Parse()

	serverAddr := fmt.Sprintf("%s:%s", cfg.host, cfg.port)
	fmt.Printf("INFO: Connecting to Valkey server at: %s\n", serverAddr)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{serverAddr}, EnableCrossSlotMGET: true, AllowUnstableSlotsForCrossSlotMGET: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to create Valkey client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	csClient, ok := client.(valkey.CrossSlotClient)
	if !ok {
		fmt.Fprintf(os.Stderr, "FATAL: The client does not implement valkey.CrossSlotClient\n")
		os.Exit(1)
	}
	fmt.Println("INFO: Valkey client connected successfully.")

	allPreparedKeys := prepareKeys(cfg.prepKeys)

	var preparedData map[string]string
	if cfg.populate {
		var err error
		preparedData, err = prepareData(context.Background(), client, allPreparedKeys, cfg.valueLength)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: Failed during data preparation: %v\n", err)
			os.Exit(1)
		}
	}

	metrics := NewLatencyHistogram(string(cfg.mode))
	mainLoopCtx, cancelMainLoop := context.WithTimeout(context.Background(), time.Duration(cfg.durationSec)*time.Second)
	defer cancelMainLoop()

	var wg sync.WaitGroup
	var totalCycles atomic.Int64
	fmt.Printf("INFO: Total keys prepared: %d, Value length: %d bytes, Number of Keys Per Batch: %d\n", len(allPreparedKeys), cfg.valueLength, cfg.numKeys)
	fmt.Printf("INFO: Starting test run with mode '%s' on %d thread(s) for %d seconds...\n", cfg.mode, cfg.threads, cfg.durationSec)

	for i := 0; i < cfg.threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
		loop:
			for {
				select {
				case <-mainLoopCtx.Done():
					break loop
				default:
					cycleNum := totalCycles.Add(1)
					runSingleCycle(context.Background(), client, csClient, allPreparedKeys, preparedData, cfg, metrics, workerID, cycleNum)
				}
			}
		}(i + 1)
	}
	wg.Wait()
	fmt.Printf("\nINFO: Test finished. Completed %v total cycles across %d thread(s).\n", totalCycles.Load(), cfg.threads)
	metrics.Print()
}

// --- MGET Execution and Verification Functions ---

// executeAndCollectWithMGet is the wrapper for the 'MGet' mode.
func executeAndCollectWithMGet(verbose bool, ctx context.Context, client valkey.Client, keys []string, workerID int, cycleNum int64) map[string]string {
	if verbose {
		fmt.Printf("[W:%d C:%d] Executing with MGet helper for %d keys...\n", workerID, cycleNum, len(keys))
	}
	actualFetchedData := make(map[string]string, len(keys))
	results, err := valkey.MGet(client, ctx, keys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[W:%d C:%d] MGet helper returned an error: %v\n", workerID, cycleNum, err)
	}

	for key, msg := range results {
		if msg.IsNil() {
			actualFetchedData[key] = NilValueString
		} else if valStr, strErr := msg.ToString(); strErr != nil {
			actualFetchedData[key] = ErrorValueString
		} else {
			actualFetchedData[key] = valStr
		}
	}
	// Ensure all requested keys are accounted for, even if they were missing from the results map
	for _, key := range keys {
		if _, ok := actualFetchedData[key]; !ok {
			actualFetchedData[key] = ErrorValueString
		}
	}
	return actualFetchedData
}

// mgetJobResult holds the outcome of a single MGET command in the Parallel Do model.
type mgetJobResult struct {
	requestedKeys []string
	fetchedValues []string
	jobError      error
}

func executeAndCollectWithDoMulti(verbose bool, ctx context.Context, client valkey.Client, mgetCmds []valkey.Completed, workerID int, cycleNum int64) map[string]string {
	// Renamed function, body is identical to previous executeAndCollectMGETsWithDoMulti
	if verbose {
		fmt.Printf("[W:%d C:%d] Executing %d MGET command(s) with DoMulti...\n", workerID, cycleNum, len(mgetCmds))
	}
	actualFetchedData := make(map[string]string)
	if len(mgetCmds) == 0 {
		return actualFetchedData
	}
	allRequestedKeys := make([][]string, len(mgetCmds))
	for i, cmd := range mgetCmds {
		cmdArgs := cmd.Commands()
		if len(cmdArgs) < 2 {
			allRequestedKeys[i] = []string{}
		} else {
			sourceKeys := cmdArgs[1:]
			copiedKeys := make([]string, len(sourceKeys))
			copy(copiedKeys, sourceKeys)
			allRequestedKeys[i] = copiedKeys
		}
	}
	results := client.DoMulti(ctx, mgetCmds...)
	for i, mgetResp := range results {
		requestedKeys := allRequestedKeys[i]
		if err := mgetResp.Error(); err != nil {
			fmt.Fprintf(os.Stderr, "[W:%d C:%d] DoMulti command #%d failed: %v. Keys: %v\n", workerID, cycleNum, i, err, requestedKeys)
			for _, key := range requestedKeys {
				actualFetchedData[key] = ErrorValueString
			}
			continue
		}
		rawValues, arrErr := mgetResp.ToArray()
		if arrErr != nil || len(rawValues) != len(requestedKeys) {
			fmt.Fprintf(os.Stderr, "[W:%d C:%d] DoMulti command #%d parse/count mismatch. Err: %v\n", workerID, cycleNum, i, arrErr)
			for _, key := range requestedKeys {
				actualFetchedData[key] = ErrorValueString
			}
			continue
		}
		for j, itemMsg := range rawValues {
			currentKey := requestedKeys[j]
			if itemMsg.IsNil() {
				actualFetchedData[currentKey] = NilValueString
			} else if valStr, strErr := itemMsg.ToString(); strErr != nil {
				actualFetchedData[currentKey] = ErrorValueString
			} else {
				actualFetchedData[currentKey] = valStr
			}
		}
	}
	return actualFetchedData
}

func executeAndCollectWithParallel(verbose bool, ctx context.Context, client valkey.Client, mgetCmds []valkey.Completed, workerID int, cycleNum int64) map[string]string {
	// Renamed function, body is identical to previous executeAndCollectMGETs
	if verbose {
		fmt.Printf("[W:%d C:%d] Executing %d MGET command(s) in Parallel...\n", workerID, cycleNum, len(mgetCmds))
	}
	actualFetchedData := make(map[string]string)
	if len(mgetCmds) == 0 {
		return actualFetchedData
	}
	resultsChan := make(chan mgetJobResult, len(mgetCmds))
	var wg sync.WaitGroup
	for i, mgetCmd := range mgetCmds {
		wg.Add(1)
		go func(cmd valkey.Completed, cmdIndex int) {
			defer wg.Done()
			jobRes := mgetJobResult{}
			cmdArgs := cmd.Commands()
			if len(cmdArgs) < 2 {
				jobRes.jobError = fmt.Errorf("command %d: invalid MGET structure: %v", cmdIndex, cmdArgs)
				resultsChan <- jobRes
				return
			}
			originalSlice := cmdArgs[1:]
			jobRes.requestedKeys = make([]string, len(originalSlice))
			copy(jobRes.requestedKeys, originalSlice)
			jobRes.fetchedValues = make([]string, len(jobRes.requestedKeys))
			mgetResp := client.Do(ctx, cmd)
			if err := mgetResp.Error(); err != nil {
				jobRes.jobError = fmt.Errorf("command %d (keys %v): exec failed: %w", cmdIndex, jobRes.requestedKeys, err)
				resultsChan <- jobRes
				return
			}
			rawValues, arrErr := mgetResp.ToArray()
			if arrErr != nil || len(rawValues) != len(jobRes.requestedKeys) {
				jobRes.jobError = fmt.Errorf("command %d (keys %v): parse/count mismatch. Err: %w", cmdIndex, jobRes.requestedKeys, arrErr)
				resultsChan <- jobRes
				return
			}
			for i, itemMsg := range rawValues {
				if itemMsg.IsNil() {
					jobRes.fetchedValues[i] = NilValueString
				} else if valStr, strErr := itemMsg.ToString(); strErr != nil {
					jobRes.fetchedValues[i] = ErrorValueString
				} else {
					jobRes.fetchedValues[i] = valStr
				}
			}
			resultsChan <- jobRes
		}(mgetCmd, i)
	}
	wg.Wait()
	close(resultsChan)
	for jobRes := range resultsChan {
		if jobRes.jobError != nil {
			fmt.Fprintf(os.Stderr, "[W:%d C:%d] Parallel job processing error: %v\n", workerID, cycleNum, jobRes.jobError)
			for _, key := range jobRes.requestedKeys {
				actualFetchedData[key] = ErrorValueString
			}
			continue
		}
		for i, key := range jobRes.requestedKeys {
			actualFetchedData[key] = jobRes.fetchedValues[i]
		}
	}
	return actualFetchedData
}

func verifyData(verbose bool, expectedKVs, actualKVs map[string]string, workerID int, cycleNum int64) {
	if verbose {
		fmt.Printf("[W:%d C:%d] Verifying %d keys...\n", workerID, cycleNum, len(expectedKVs))
	}
	mismatches := 0
	for expectedKey, expectedValue := range expectedKVs {
		actualValue, found := actualKVs[expectedKey]
		if !found {
			fmt.Fprintf(os.Stderr, "[W:%d C:%d] VERIFY FAIL: Key '%s' was expected but NOT FOUND.\n", workerID, cycleNum, expectedKey)
			mismatches++
			continue
		}
		if actualValue != expectedValue {
			fmt.Fprintf(os.Stderr, "[W:%d C:%d] VERIFY FAIL: Key '%s' DATA MISMATCH. Expected: '%s', Got: '%s'\n", workerID, cycleNum, expectedKey, expectedValue, actualValue)
			mismatches++
		}
	}
	if mismatches > 0 {
		fmt.Fprintf(os.Stderr, "[W:%d C:%d] VERIFY SUMMARY: Found %d mismatches/errors.\n", workerID, cycleNum, mismatches)
	} else if verbose {
		fmt.Printf("[W:%d C:%d] VERIFY PASS: All %d keys matched.\n", workerID, cycleNum, len(expectedKVs))
	}
}
