# Valkey-go Cross-Slot MGET Example

This example demonstrates how to use the `BuildCrossSlotMGETs` feature of the `valkey-go` client to perform `MGET`
operations for keys distributed across different slots in a Valkey cluster.

The program performs the following steps in a loop for a configurable duration:
1.  Generates a specified number of unique keys and random string values.
2.  Sets these key-value pairs into the Valkey cluster.
3.  Uses the `BuildCrossSlotMGETs` method on a `CrossSlotClient` (typically a `clusterClient`) to create a
    list of `MGET` commands. These commands are grouped optimally to target the specific nodes holding the keys.
4.  Executes these `MGET` commands in parallel.
5.  Verifies that the values retrieved by `MGET` match the values that were initially set.
6.  Reports on the success or failure of the verification.

## Prerequisites

* **Go**: Version 1.20 or higher.
* **Valkey Cluster**: A running Valkey cluster accessible from where you run the example. The example is designed
to work with a clustered setup to showcase cross-slot functionality.

## Running the Example

1.  **Navigate to the example directory**:
    Open your terminal and change to the directory containing this example:
    ```bash
    cd path/to/valkey-go/examples/xslot-mget
    ```

2.  **Run the Go program**:
    You can run the example using `go run`. The `go.work` file in this directory ensures that the example can
    find the main `valkey-go` module (located two directories above).

    ```bash
    go run xslot-mget.go [flags]
    ```

    **Available Flags**:

    * `-host <string>`: Valkey server host IP/hostname (default: "127.0.0.1"). For a cluster,
       this should be the address of one of the cluster nodes.
    * `-port <string>`: Valkey server port (default: "6379").
    * `-duration <int>`: Test duration in seconds (default: 10). The test will run cycles of
       SET, MGET, and VERIFY for this duration.
    * `-numkeys <int>`: Number of keys to generate, SET, and MGET per cycle (default: 10).

    **Example Usage**:

    * Run with default settings (connects to `127.0.0.1:6379`, runs for 10 seconds, 10 keys per cycle):
        ```bash
        go run xslot-mget.go
        ```

    * Run against a specific cluster node, for 60 seconds, with 100 keys per cycle:
        ```bash
        go run xslot-mget.go -host your-cluster-node-ip -port 6379 -duration 60 -numkeys 100
        ```

## Expected Output

The program will print logs to the console indicating:
* Connection attempt to the Valkey server.
* Start and end of each test cycle.
* Number of keys being generated, set, and MGET-ed.
* Number of MGET commands built (this shows how keys were grouped per node/slot).
* Status of data verification (PASS or FAIL with details on mismatches, missing keys, or errors).
* A summary at the end of the test duration.

If any verification step fails, the program will print detailed error messages and exit with a non-zero status code.
