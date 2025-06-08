//go:build pprof
// +build pprof

package main
  
import (
    _ "net/http/pprof"      // registers /debug/pprof/*
    "log"
    "net/http"
    "runtime"
)

// init runs **before** main().
func init() {
    // ❶ Make profiles richer (1 = record every event)
    runtime.SetBlockProfileRate(1)          // goroutine blocking          :contentReference[oaicite:0]{index=0}
    runtime.SetMutexProfileFraction(1)      // mutex contention             :contentReference[oaicite:1]{index=1}

    // ❷ Expose them on localhost:6060
    go func() {
        log.Println("pprof at http://localhost:6060/debug/pprof/")
        log.Println(http.ListenAndServe("localhost:6060", nil))
    }()
}

