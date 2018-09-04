package main

import (
	"github.com/go-redis/redis"
	"fmt"
	"os"
	"regexp"
	"sync"
	"sort"
)

const (
	REDIS_HOST = "127.0.0.1:6378"
	REDIS_DB   = 3
)

var rs *redis.Client
var resultchan = make(chan string, 1000)

func init() {

	rs = redis.NewClient(&redis.Options{
		Addr: REDIS_HOST,
		DB:   REDIS_DB,
	})
	_, err := rs.Ping().Result()
	if err != nil {
		panic(err)
	}
}

func IsValidUUID(uuid string) bool {
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
}

// TODO use pipeline
func search(key, value string, wg *sync.WaitGroup) {
	defer wg.Done()
	exists, err := rs.Do("bf.exists", key, value).Int()
	if err != nil {
		panic(err)
	}
	if exists == 0 {
		return
	}
	resultchan <- key[5:]
}

func main() {
	uuid := os.Args[1]
	sortResult := []string{}

	if ! IsValidUUID(uuid) {
		panic("Please input valid uuid")
	}
	fmt.Println(uuid)
	result, err := rs.Keys("uuid*").Result()
	if err != nil {
		panic(err)
	}

	maxGoroutines := 20
	guard := make(chan struct{}, maxGoroutines)
	var wg sync.WaitGroup

	for _, i := range result {
		wg.Add(1)
		go func(i string) {
			search(i, uuid, &wg)
			<-guard
		}(i)
	}
	go func() {
		for i := range resultchan {
			sortResult = append(sortResult, i)
		}
	}()
	wg.Wait()
	close(resultchan)
	sort.Strings(sortResult)
	for _, i := range sortResult {

		fmt.Println(i)
	}
}

