// Command fakeprom is a tiny Prometheus API stand-in for the HA test and the demo.
// Queries that mention probe_success return one sample per probe region with the
// value from $STATE_DIR/probe (default 1). Queries that mention "serial" return one
// unix-time serial per server, $STATE_DIR/serial_lag seconds old (default 60).
// Every other query returns a single sample with the value from $STATE_DIR/value
// (default 1). Change the files to simulate an outage. Listens on $LISTEN (default :9090).
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func read(dir, name, def string) string {
	b, err := os.ReadFile(dir + "/" + name)
	if err != nil {
		return def
	}
	return strings.TrimSpace(string(b))
}

func main() {
	dir := os.Getenv("STATE_DIR")
	if dir == "" {
		dir = "/state"
	}
	http.HandleFunc("/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		q := r.Form.Get("query")
		now := float64(time.Now().Unix())
		type sample struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		}
		var res []sample
		if strings.Contains(q, "probe_success") {
			v := read(dir, "probe", "1")
			for _, reg := range []string{"eu", "us", "apac"} {
				res = append(res, sample{Metric: map[string]string{"probe_region": reg}, Value: [2]any{now, v}})
			}
		} else if strings.Contains(q, "serial") {
			lag, err := strconv.ParseFloat(read(dir, "serial_lag", "60"), 64)
			if err != nil {
				lag = 60
			}
			for _, srv := range []string{"ns1", "ns2", "ns3"} {
				res = append(res, sample{Metric: map[string]string{"instance": srv}, Value: [2]any{now, strconv.FormatFloat(now-lag, 'f', 0, 64)}})
			}
		} else {
			v := read(dir, "value", "1")
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				v = "1"
			}
			res = append(res, sample{Metric: map[string]string{}, Value: [2]any{now, v}})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": res}})
	})
	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = ":9090"
	}
	fmt.Println("fakeprom listening on", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
