package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"log"
	"math/rand"
	"net/http"
	"reflect"
	"time"
)

type Geopoint struct {
	Lat float64 `json:lat`
	Lon float64 `json:lon`
}

func QueryString(key, field string, limit int) string {
	var searchQuery = `{
	"query": {
		"filtered": {
			"query": {
				"query_string": {
					"query": "%s"
				}
			}
		}
	},
	"fields": [ "%s", "timestamp"
	],
	"from": 0,
	"size": %d,
	"sort": {
		"_score": {
			"order": "asc"
		}
	},
	"explain": true
}`
	return fmt.Sprintf(searchQuery, key, field, limit)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}

func queryES(query string, tag string, timing bool) (elastigo.SearchResult, error) {
	if timing {
		defer timeTrack(time.Now(), "Query")
	}
	c := elastigo.NewConn()
	//c.Domain = "10.2.1.8"
	//hosts := []string{"10.2.1.8"}
	hosts := []string{"10.2.20.6", "10.2.20.7", "10.2.1.6", "10.2.1.7", "10.2.1.8"}
	c.SetHosts(hosts)
	return c.Search("tfotos", "", nil, query)
}

func QueryWithTimeout(query string, tag string, timeout_seconds time.Duration) (
	elastigo.SearchResult, error) {
	defer timeTrack(time.Now(), "Query")
	timeout_timer := time.NewTimer(timeout_seconds)
	defer timeout_timer.Stop()
	result_ch := make(chan elastigo.SearchResult, 1)
	error_ch := make(chan error, 1)

	start_time := time.Now()
	go func() {
		c := elastigo.NewConn()
		c.Domain = "10.2.1.8"
		result, err := c.Search("tfotos", "", nil, query)
		result_ch <- result
		if err != nil {
			error_ch <- err
		}
	}()

	var retval elastigo.SearchResult

	select {
	case <-timeout_timer.C:
		return retval, errors.New(fmt.Sprintf("E-TIMEOUT %v", timeout_seconds))
	default:
	}

	select {
	case <-timeout_timer.C: // time.After(timeout_seconds):
		return retval, errors.New(fmt.Sprintf("L-TIMEOUT %v", timeout_seconds))
	case error := <-error_ch:
		return retval, error
	case response := <-result_ch:
		time_b := time.Since(start_time)
		if time_b > timeout_seconds {
			fmt.Println("WTF", time_b, timeout_seconds)
		}
		return response, nil
	}
}

func getNames(num_res int) []string {
	var values []string
	var searchQuery = QueryString("*J*", "user", num_res)
	fmt.Println(searchQuery)
	response, err := queryES(searchQuery, "*J*", true) //, 50*time.Second /*timeout seconds*/)
	if err != nil {
		log.Fatalf("The search of photo id has failed:", err)
	}
	fmt.Println(reflect.TypeOf(response))
	fmt.Println("Number of search result:", len(response.Hits.Hits))

	for _, v := range response.Hits.Hits {
		var value map[string]interface{}
		err := json.Unmarshal([]byte(*v.Fields), &value)
		if err != nil {
			log.Fatalf("Failed to unmarshal", err)
		}
		vv := value["user"]
		if string_s, ok := vv.([]interface{}); ok {
			if str, is_str := string_s[0].(string); is_str {
				values = append(values, str)
				fmt.Println(str)
			}
		} else {
			fmt.Println("Error: not string")
			fmt.Println(vv, "type", reflect.TypeOf(vv))
		}
	}
	return values
}

func blockingQueries(numQueries int) {
	values := getNames(numQueries)

	fmt.Println("Start load testing with num queries", len(values))
	stime := time.Now()
	n_sent := 0
	n_err := 0
	n_done := 0
	// Typically a non-fixed seed should be used, such as time.Now().UnixNano().
	// Using a fixed seed will produce the same output on every run.
	r := rand.New(rand.NewSource(99))

	for {
		n_sent += 1
		index := r.Intn(numQueries)
		v := values[index]
		var q = QueryString(v, "user", 20)
		response, err := queryES(q, v, false)
		if err != nil || len(response.Hits.Hits) <= 1 {
			n_err += 1
			fmt.Println("Error!!!!", err, len(response.Hits.Hits))
		} else {
			n_done += 1
		}

		if n_sent%100 == 0 {
			elapsed := time.Since(stime).Seconds()
			fmt.Printf("Total num of query %v finished %v err %v qps %f \n",
				n_sent, n_done, n_err, float64(n_done)/elapsed)
		}
	}
}

func manyQueries(numQueries int) {
	// Load test:
	i_s := 0
	i_e := 0
	i_err := 0
	const NCPU = 8
	c := make(chan int, NCPU)

	values := getNames(numQueries)

	stime := time.Now()
	timeout_seconds := time.Second * 10

	disable_header := false
	if disable_header {
		http.DefaultTransport.(*http.Transport).ResponseHeaderTimeout = timeout_seconds
	}

	fmt.Println("Start load testing with num queries", len(values))
	for _, val := range values {
		go func(v string, c chan int) {
			i_s = i_s + 1
			var q = QueryString(v, "user", 20)
			response, err := queryES(q, v, true)
			fmt.Println("Result:", len(response.Hits.Hits))
			c <- 1
			i_e = i_e + 1
			if err != nil {
				i_err = i_err + 1
				fmt.Println("Error!!!!", err)
			}
		}(val, c)
	}

	total := 0
	stop := false
	defer timeTrack(stime, "Total time")

	timeout_timer := time.NewTimer(20 * time.Second)
	defer timeout_timer.Stop()
	for !stop {
		select {
		case val := <-c:
			total += val
			stop = total == len(values)
		case <-timeout_timer.C:
			stop = true
			fmt.Println("Final Timeout")
		}
	}

	defer fmt.Printf("Total num of query %v: started %v finished %v err %v qps %f \n",
		total, i_s, i_e, i_err, float64(total)/time.Since(stime).Seconds())
}

func main() {
	numQueries := flag.Int("num_query", 4900, "number of queris")
	flag.Parse()

	//manyQueries(*numQueries)
	blockingQueries(*numQueries)
}
