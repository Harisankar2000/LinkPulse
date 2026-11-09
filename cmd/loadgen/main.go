// Command loadgen creates some links, then hammers the redirect endpoint with
// concurrent workers and prints throughput and latency percentiles.
//
//	go run ./cmd/loadgen -target http://localhost:8080 -c 64 -d 20s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"
)

func main() {
	target := flag.String("target", "http://localhost:8080", "API base URL")
	concurrency := flag.Int("c", 64, "concurrent workers")
	duration := flag.Duration("d", 20*time.Second, "test duration")
	numLinks := flag.Int("links", 200, "links to create before the test")
	flag.Parse()

	transport := &http.Transport{MaxIdleConns: *concurrency, MaxIdleConnsPerHost: *concurrency}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	codes, err := createLinks(client, *target, *numLinks)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup failed:", err)
		os.Exit(1)
	}
	fmt.Printf("created %d links; running %d workers for %s\n", len(codes), *concurrency, *duration)

	type result struct {
		latencies []time.Duration
		status    map[int]int
		errors    int
	}
	results := make([]result, *concurrency)
	deadline := time.Now().Add(*duration)

	var wg sync.WaitGroup
	for w := range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := result{status: map[int]int{}}
			for time.Now().Before(deadline) {
				// Skew traffic like real life: a few links get most of the clicks.
				code := codes[min(int(rand.ExpFloat64()*float64(len(codes))/8), len(codes)-1)]
				start := time.Now()
				resp, err := client.Get(*target + "/" + code)
				if err != nil {
					res.errors++
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				res.latencies = append(res.latencies, time.Since(start))
				res.status[resp.StatusCode]++
			}
			results[w] = res
		}()
	}
	wg.Wait()

	var all []time.Duration
	status := map[int]int{}
	errs := 0
	for _, r := range results {
		all = append(all, r.latencies...)
		for k, v := range r.status {
			status[k] += v
		}
		errs += r.errors
	}
	slices.Sort(all)
	if len(all) == 0 {
		fmt.Println("no successful requests")
		os.Exit(1)
	}
	pct := func(p float64) time.Duration { return all[min(int(float64(len(all))*p), len(all)-1)] }

	fmt.Printf("\nrequests: %d   errors: %d   throughput: %.0f req/s\n",
		len(all), errs, float64(len(all))/duration.Seconds())
	fmt.Printf("latency  p50: %v   p90: %v   p99: %v   max: %v\n",
		pct(0.50).Round(time.Microsecond), pct(0.90).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond), all[len(all)-1].Round(time.Microsecond))
	fmt.Printf("status codes: %v\n", status)
	if status[http.StatusTooManyRequests] > 0 {
		fmt.Println("note: 429s mean the redirect rate limiter is on; start the API with RATE_REDIRECT_RPS=0 to benchmark raw throughput")
	}
}

func createLinks(client *http.Client, target string, n int) ([]string, error) {
	codes := make([]string, 0, n)
	for i := range n {
		body, _ := json.Marshal(map[string]string{"url": fmt.Sprintf("https://example.com/page/%d", i)})
		resp, err := client.Post(target+"/api/v1/links", "application/json", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		var out struct {
			Code string `json:"code"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, fmt.Errorf("create rate limit hit after %d links; start the API with RATE_CREATE_PER_MIN=0", i)
		}
		if err != nil || resp.StatusCode != http.StatusCreated {
			return nil, fmt.Errorf("create link: status %d, err %v", resp.StatusCode, err)
		}
		codes = append(codes, out.Code)
	}
	return codes, nil
}
