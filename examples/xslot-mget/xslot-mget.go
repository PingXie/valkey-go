package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort" // For sorting generated keys if desired for logging consistency
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

const (
	keyPrefix        = "csmget_test" // Prefix for generated keys
	valueLength      = 10            // Length of random string values
	NilValueString   = "(nil)"       // Representation for nil values from MGET
	ErrorValueString = "<error_val>" // Representation for values that couldn't be parsed/fetched
)

// randString generates a random string of a given length.
func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano())) // Ensure fresh rand source if called rapidly
	for i := range b {
		b[i] = letterBytes[seededRand.Intn(len(letterBytes))]
	}
	return string(b)
}

// generateData creates a map of random keys and values, and a slice of all keys.
func generateData(numKeys int, cycleNum int) (expectedKVs map[string]string, allKeys []string) {
	expectedKVs = make(map[string]string, numKeys)
	allKeys = make([]string, 0, numKeys)
	// Using a more unique component for keys across different runs/cycles if needed
	uniqueComponent := time.Now().UnixNano()

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("%s_c%d_u%d_k%d", keyPrefix, cycleNum, uniqueComponent, i)
		value := randString(valueLength)
		expectedKVs[key] = value
		allKeys = append(allKeys, key)
	}
	// Sorting keys can make logs slightly easier to follow, though not strictly necessary.
	sort.Strings(allKeys)
	return expectedKVs, allKeys
}

// setData SETs the given key-value pairs into Valkey.
func setData(ctx context.Context, client valkey.Client, kvs map[string]string, cycleNum int) {
	fmt.Printf("[%d] Setting %d keys...\n", cycleNum, len(kvs))
	for k, v := range kvs {
		cmd := client.B().Set().Key(k).Value(v).Build()
		err := client.Do(ctx, cmd).Error()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%d] Error SETTING key '%s' to value '%s': %v\n", cycleNum, k, v, err)
			// Continue trying to set other keys
		}
	}
	fmt.Printf("[%d] Finished setting keys.\n", cycleNum)
}

// mgetJobResult holds the outcome of a single MGET command (which might fetch multiple keys).
type mgetJobResult struct {
	requestedKeys []string // Keys requested by this specific MGET command call
	fetchedValues []string // Values fetched, in the same order as requestedKeys.
	jobError      error    // Error for the whole MGET job (e.g., execution failure)
}

// executeAndCollectMGETs executes a list of MGET commands in parallel and aggregates their results.
func executeAndCollectMGETs(ctx context.Context, client valkey.Client, mgetCmds []valkey.Completed, cycleNum int) map[string]string {
	fmt.Printf("[%d] Executing %d MGET command(s) in parallel...\n", cycleNum, len(mgetCmds))
	actualFetchedData := make(map[string]string) // Stores key -> fetched_value_string

	if len(mgetCmds) == 0 {
		fmt.Printf("[%d] No MGET commands to execute.\n", cycleNum)
		return actualFetchedData
	}

	resultsChan := make(chan mgetJobResult, len(mgetCmds))
	var wg sync.WaitGroup

	for i, mgetCmd := range mgetCmds {
		wg.Add(1)
		go func(cmd valkey.Completed, cmdIndex int) {
			defer wg.Done()
			jobRes := mgetJobResult{}

			cmdArgs := cmd.Commands()                                      // Expected: ["MGET", "keyA", "keyB", ...]
			if len(cmdArgs) < 1 || strings.ToUpper(cmdArgs[0]) != "MGET" { // MGET itself needs at least one key argument to be valid
				jobRes.jobError = fmt.Errorf("command %d: invalid MGET command structure from BuildCrossSlotMGETs: %v", cmdIndex, cmdArgs)
				resultsChan <- jobRes
				return
			}
			if len(cmdArgs) == 1 { // Just "MGET" with no keys
				// This is technically a valid command but will result in an error from the server.
				// Let client.Do handle it, or could catch it here. For now, let it pass to Do.
				jobRes.requestedKeys = []string{}
			} else {
				originalSlice := cmdArgs[1:]
				jobRes.requestedKeys = make([]string, len(originalSlice))
				copy(jobRes.requestedKeys, originalSlice)
			}
			jobRes.fetchedValues = make([]string, len(jobRes.requestedKeys))

			// fmt.Printf("[%d] Goroutine %d: Executing MGET for keys: %v\n", cycleNum, cmdIndex, jobRes.requestedKeys)
			mgetResp := client.Do(ctx, cmd) // client is valkey.Client

			if err := mgetResp.Error(); err != nil {
				jobRes.jobError = fmt.Errorf("command %d (keys %v): MGET execution failed: %w", cmdIndex, jobRes.requestedKeys, err)
				resultsChan <- jobRes
				return
			}

			rawValues, arrErr := mgetResp.ToArray() // []valkey.ValkeyMessage
			if arrErr != nil {
				jobRes.jobError = fmt.Errorf("command %d (keys %v): MGET ToArray() failed: %w. Raw response: %s", cmdIndex, jobRes.requestedKeys, arrErr, mgetResp.String())
				resultsChan <- jobRes
				return
			}

			if len(rawValues) != len(jobRes.requestedKeys) {
				jobRes.jobError = fmt.Errorf("command %d (keys %v): MGET result count mismatch: expected %d, got %d. Raw response: %s",
					cmdIndex, jobRes.requestedKeys, len(jobRes.requestedKeys), len(rawValues), mgetResp.String())
				resultsChan <- jobRes
				return
			}

			for i, itemMsg := range rawValues {
				currentKey := jobRes.requestedKeys[i]
				if itemMsg.IsNil() {
					jobRes.fetchedValues[i] = NilValueString
				} else {
					valStr, strErr := itemMsg.ToString()
					if strErr != nil {
						fmt.Fprintf(os.Stderr, "[%d] Cmd %d: Error converting MGET result for key '%s' (part of MGET for %v): %v\n",
							cycleNum, cmdIndex, currentKey, jobRes.requestedKeys, strErr)
						jobRes.fetchedValues[i] = ErrorValueString // Mark this specific value as errored
					} else {
						jobRes.fetchedValues[i] = valStr
					}
				}
			}
			resultsChan <- jobRes
		}(mgetCmd, i)
	}

	wg.Wait()
	close(resultsChan)

	fmt.Printf("[%d] Collecting and aggregating MGET results...\n", cycleNum)
	for jobRes := range resultsChan {
		if jobRes.jobError != nil {
			fmt.Fprintf(os.Stderr, "[%d] MGET job processing error: %v\n", cycleNum, jobRes.jobError)
			// Mark all keys requested by this failed job as having errored in retrieval
			for _, key := range jobRes.requestedKeys {
				actualFetchedData[key] = ErrorValueString
			}
			continue // Skip to the next job result
		}

		for i, key := range jobRes.requestedKeys {
			// Ensure index is within bounds, though previous checks should guarantee it.
			if i < len(jobRes.fetchedValues) {
				actualFetchedData[key] = jobRes.fetchedValues[i]
			} else {
				fmt.Fprintf(os.Stderr, "[%d] Warning: MGET results slice shorter than requested keys for key '%s'. Marking as error.\n", cycleNum, key)
				actualFetchedData[key] = ErrorValueString
			}
		}
	}
	fmt.Printf("[%d] Finished collecting MGET results. Total unique keys in fetched map: %d\n", cycleNum, len(actualFetchedData))
	return actualFetchedData
}

// verifyData compares expected key-value pairs with actual fetched data.
func verifyData(expectedKVs, actualKVs map[string]string, cycleNum int) {
	fmt.Printf("[%d] Verifying %d expected keys...\n", cycleNum, len(expectedKVs))
	mismatches := 0
	missingInActual := 0      // Keys expected but not found in MGET results at all
	nilInsteadOfValue := 0    // Keys expected with a value but MGET returned (nil)
	errorRetrievingValue := 0 // Keys for which MGET returned an error string

	for expectedKey, expectedValue := range expectedKVs {
		actualValue, found := actualKVs[expectedKey]
		if !found {
			fmt.Fprintf(os.Stderr, "[%d] VERIFY FAIL: Key '%s' was SET (expected value '%s') but NOT FOUND in MGET results.\n", cycleNum, expectedKey, expectedValue)
			missingInActual++
			continue
		}
		if actualValue == NilValueString {
			fmt.Fprintf(os.Stderr, "[%d] VERIFY FAIL: Key '%s' expected value '%s', but MGET returned '%s'.\n", cycleNum, expectedKey, expectedValue, NilValueString)
			nilInsteadOfValue++
			// This is a form of mismatch if a non-nil value was expected.
			mismatches++
			continue
		}
		if actualValue == ErrorValueString {
			fmt.Fprintf(os.Stderr, "[%d] VERIFY INFO: Key '%s' (expected value '%s') could not be properly verified due to an earlier MGET error ('%s').\n", cycleNum, expectedKey, expectedValue, ErrorValueString)
			errorRetrievingValue++
			// Not a data mismatch per se, but a retrieval failure.
			continue
		}
		if actualValue != expectedValue {
			fmt.Fprintf(os.Stderr, "[%d] VERIFY FAIL: Key '%s' DATA MISMATCH. Expected: '%s', Got: '%s'\n", cycleNum, expectedKey, expectedValue, actualValue)
			mismatches++
		}
	}

	extraInActual := 0
	for actualKey := range actualKVs {
		if _, expected := expectedKVs[actualKey]; !expected {
			// This should ideally not happen if MGETs are only for keys we set.
			fmt.Fprintf(os.Stderr, "[%d] VERIFY WARN: Key '%s' (value '%s') was fetched by MGET but was NOT in the expected set for this cycle.\n", cycleNum, actualKey, actualKVs[actualKey])
			extraInActual++
		}
	}

	if mismatches == 0 && missingInActual == 0 && nilInsteadOfValue == 0 && errorRetrievingValue == 0 && extraInActual == 0 {
		fmt.Printf("[%d] VERIFY PASS: All %d keys and values matched successfully.\n", cycleNum, len(expectedKVs))
	} else {
		fmt.Printf("[%d] VERIFY SUMMARY (out of %d expected keys):\n"+
			"  - Data Mismatches (value different or was nil when value expected): %d\n"+
			"  - Keys Missing in MGET results: %d\n"+
			"  - Keys With Error during MGET retrieval/parsing: %d\n"+
			"  - Unexpected Extra Keys in MGET results: %d\n",
			cycleNum, len(expectedKVs), mismatches, missingInActual, errorRetrievingValue, extraInActual)
		os.Exit(1) // Exit with error code if any mismatches or issues found
	}
}

// runSingleCycle performs one iteration of data generation, SET, MGET, and Verify.
func runSingleCycle(baseCtx context.Context, client valkey.Client, crossSlotClient valkey.CrossSlotClient, numKeys int, cycleNum int) {
	fmt.Printf("\n[%d] === Starting Test Cycle ===\n", cycleNum)
	// Create a context for this cycle's operations, can be Background or derived.
	cycleCtx := baseCtx // For now, use the passed context (which might be Background)

	// 1. Generate Data
	expectedKVs, allKeys := generateData(numKeys, cycleNum)
	fmt.Printf("[%d] Generated %d key-value pairs.\n", cycleNum, len(expectedKVs))

	// 2. SET Data
	setData(cycleCtx, client, expectedKVs, cycleNum)

	// 3. Build MGET Commands
	fmt.Printf("[%d] Building cross-slot MGET commands for %d keys.\n", cycleNum, len(allKeys))
	// For debugging: fmt.Printf("[%d] Keys for MGET: %v\n", cycleNum, allKeys)
	mgetCmds, err := crossSlotClient.BuildCrossSlotMGETs(cycleCtx, allKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%d] CRITICAL: Error building MGET commands: %v. Skipping MGET and Verify for this cycle.\n", cycleNum, err)
		return
	}
	fmt.Printf("[%d] Built %d MGET command(s) to fetch the keys.\n", cycleNum, len(mgetCmds))
	// For verbose debugging of generated MGET commands:
	// for i, cmd := range mgetCmds {
	// 	fmt.Printf("[%d]   MGET Cmd %d targets keys: %v\n", cycleNum, i, cmd.Commands()[1:])
	// }

	// 4. Execute MGETs in parallel and collect results
	actualFetchedData := executeAndCollectMGETs(cycleCtx, client, mgetCmds, cycleNum)

	// 5. Verify Data
	verifyData(expectedKVs, actualFetchedData, cycleNum)
	fmt.Printf("[%d] === Finished Test Cycle ===\n", cycleNum)
}

func main() {
	host := flag.String("host", "127.0.0.1", "Valkey server host IP/hostname")
	port := flag.String("port", "6379", "Valkey server port")
	durationSec := flag.Int("duration", 10, "Test duration in seconds")
	numKeys := flag.Int("numkeys", 10, "Number of keys to MGET per cycle")
	flag.Parse()

	// Seed random number generator once at the start.
	rand.Seed(time.Now().UnixNano())

	serverAddr := fmt.Sprintf("%s:%s", *host, *port)
	fmt.Printf("Attempting to connect to Valkey server at: %s\n", serverAddr)

	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{serverAddr}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Valkey client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	fmt.Println("Valkey client created successfully.")

	crossSlotCapableClient, ok := client.(valkey.CrossSlotClient)
	if !ok {
		fmt.Fprintf(os.Stderr, "FATAL: The connected Valkey client (type %T) does not implement the valkey.CrossSlotClient interface required for BuildCrossSlotMGETs. Ensure you are using a compatible client (e.g., a cluster client if appropriate).\n", client)
		os.Exit(1)
	}
	fmt.Println("Client successfully asserted to valkey.CrossSlotClient interface.")

	// Main loop controlled by duration
	mainLoopCtx, cancelMainLoop := context.WithTimeout(context.Background(), time.Duration(*durationSec)*time.Second)
	defer cancelMainLoop()

	cycleNum := 0
	fmt.Printf("\nStarting test run for %d seconds, with %d keys per cycle.\n", *durationSec, *numKeys)

loop:
	for {
		select {
		case <-mainLoopCtx.Done():
			fmt.Println("\nTest duration elapsed. Exiting main loop.")
			break loop
		default:
			cycleNum++
			// Each cycle gets a fresh background context for its operations.
			// If needed, child contexts with deadlines could be derived from mainLoopCtx.
			runSingleCycle(context.Background(), client, crossSlotCapableClient, *numKeys, cycleNum)
			// Optional: Add a small delay between cycles if you don't want them back-to-back.
			// time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Println("\nProgram finished.")
}
