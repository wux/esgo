package esgo

import (
	"encoding/json"
	"errors"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"log"
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

func Query(query string) (elastigo.SearchResult, error) {
	defer timeTrack(time.Now(), "Query ")
	result_ch := make(chan elastigo.SearchResult, 1)
	error_ch := make(chan error, 1)
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
	case error := <-error_ch:
		//panic("error....")
		return retval, error
	case response := <-result_ch:
		return response, nil
	case <-time.After(50 * time.Second):
		// panic("Timed out...")
		return retval, errors.New("TIMEOUT")
	}
	//	return retval, errors.New("UNKNOWN")
	/*
		c := elastigo.NewConn()
		c.Domain = "10.2.1.8"
		response, err := c.Search("tfotos", "", nil, query)
		if err != nil {
			log.Fatalf("The search of photo id has failed:", err)
		}
		return response, err
	*/
}

func run_esgo() {
	var searchQuery = QueryString("*J*", "user", 4500) //940)
	fmt.Println(searchQuery)
	response, err := Query(searchQuery)
	if err != nil {
		log.Fatalf("The search of photo id has failed:", err)
	}
	fmt.Println(reflect.TypeOf(response))
	var values []string
	fmt.Println("number of search result:", len(response.Hits.Hits))

	for _, v := range response.Hits.Hits {
		//fmt.Println(k, v)
		var value map[string]interface{}
		err := json.Unmarshal([]byte(*v.Fields), &value)
		if err != nil {
			log.Fatalf("Failed to unmarshal", err)
		}
		//fmt.Println(value, value["user"])
		vv := value["user"]

		/*
			tt := value["timestamp"]
			if string_tt, ok := tt.([]interface{}); ok {
				if string_tt_s, ok := string_tt[0].(string); ok {
					fmt.Println(string_tt_s)
				}
			}*/

		if string_s, ok := vv.([]interface{}); ok {
			if str, is_str := string_s[0].(string); is_str {
				values = append(values, str)
			}
		} else {
			fmt.Println("not string")
			fmt.Println(vv, "type", reflect.TypeOf(vv))
		}
	}

	// Load test:
	i_s := 0
	i_e := 0
	i_err := 0
	//const NCPU = 8
	c := make(chan int) //, NCPU)
	//	stime := time.Now()

	// Set time out to 10s for http response.
	/*	transport := &httpclient.Transport{
			ConnectTimeout: 1*time.Second,
			ResponseHeaderTimeout: 5*time.Second,
			RequestTimeout: 10*time.Second,
		}
		defer transport.Close()
		http.DefaultTransport.(*http.Transport) = transport
	*/
	http.DefaultTransport.(*http.Transport).ResponseHeaderTimeout = time.Second * 10

	fmt.Println("Start load testing with num queries", len(values))
	for _, val := range values {
		go func(v string, c chan int) {
			//fmt.Println(v)
			i_s = i_s + 1
			var q = QueryString(v, "user", 20)
			_, err := Query(q)
			//response, err := Query(q)
			//fmt.Println("Result:", len(response.Hits.Hits))
			i_e = i_e + 1
			if err != nil {
				i_err = i_err + 1
				fmt.Println("Error!!!!", err)
			}
			/*for _, v := range response.Hits.Hits {
				fmt.Println(v)
			}*/
			c <- 1
		}(val, c)
	}

	total := 0
	timeout := false
	for !timeout {
		select {
		case val := <-c:
			total += val
		case <-time.After(20 * time.Second):
			timeout = true
			fmt.Println("timeout 200s")
		}
	}
	fmt.Printf("T: s %v e %v err %v total %v\n", i_s, i_e, i_err, total)
	//time.Sleep(4 * time.Second)
	//defer timeTrack(stime, fmt.Sprintf("Total num of query s %v e %v", i_s, i_e))
	/*
		for i_e + i_err < i_s {
			fmt.Printf("T: s %v e %v err %v\n", i_s, i_e, i_err)
			time.Sleep(1 * time.Second)
		}*/

	//jsonV2, err := json.Marshal(values)
	//if err != nil {
	//	log.Fatalf("Failed marshalling:", err)
	//}
	//fmt.Println(string(jsonV2))

}
