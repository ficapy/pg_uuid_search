package main

import (
	"github.com/jmoiron/sqlx"
	"github.com/go-redis/redis"
	_ "github.com/lib/pq"

	"fmt"
	"sync"
	"sync/atomic"
	"database/sql"
)

const (
	DB_USER     = "ficapy"
	DB_PASSWORD = "123"
	DB_NAME     = "postgres"
	DB_HOST     = "127.0.0.1"

	REDIS_HOST = "127.0.0.1:6379"
	REDIS_DB   = 3
)

type UUIDColumn struct {
	schema string
	table  string
	column string
}

var db *sqlx.DB
var rs *redis.Client

func init() {
	var dbsource = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
	db, _ = sqlx.Open("postgres", dbsource)
	err := db.Ping()
	if err != nil {
		panic(err)
	}
	rs = redis.NewClient(&redis.Options{
		Addr: REDIS_HOST,
		DB:   REDIS_DB,
	})
	_, err = rs.Ping().Result()
	if err != nil {
		panic(err)
	}
	_, err = rs.Do("bf.exists", "a", 1).Int()
	if err != nil {
		fmt.Println("Redis not support bloom filter https://redislabs.com/blog/rebloom-bloom-filter-datatype-redis/")
		panic(err)
	}
}

func getColumn() ([]UUIDColumn) {
	rows, err := db.Query(`SELECT t.table_schema,c.table_name,c.column_name
FROM information_schema.columns c
left join information_schema.tables t on t.table_schema = c.table_schema and t.table_name = c.table_name
where c.data_type = 'uuid' and t.table_type = 'BASE TABLE'`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	ret := []UUIDColumn{}
	for rows.Next() {
		var schema, table, column string
		err = rows.Scan(&schema, &table, &column)
		if err != nil {
			panic(err)
		}
		ret = append(ret, UUIDColumn{
			schema: schema,
			table:  table,
			column: column,
		})
	}
	return ret
}

func getRecord(column, schema, table string, wg *sync.WaitGroup, remain *int32) {
	key := fmt.Sprintf("new_%s.%s.%s", schema, table, column)

	defer func() {
		atomic.AddInt32(remain, -1)
		fmt.Printf("Done:%s Remain:%d\n", key, *remain)
		wg.Done()
	}()

	query := fmt.Sprintf(`select distinct(%s) from %s.%s`, column, schema, table)
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value sql.NullString
		var insert string

		err = rows.Scan(&value)
		if err != nil {
			panic(err)
		}
		if value.Valid {
			insert = value.String
		} else {
			insert = "null"
		}
		_, err = rs.Do("bf.add", key, insert).Int()
		if err != nil {
			panic(err)
		}
	}
}

func renameKey(column, schema, table string) {
	old := fmt.Sprintf("new_%s.%s.%s", schema, table, column)
	new := fmt.Sprintf("uuid_%s.%s.%s", schema, table, column)
	rs.Rename(old, new)
}

func main() {
	maxGoroutines := 7
	guard := make(chan struct{}, maxGoroutines)
	var wg sync.WaitGroup

	allColumn := getColumn()
	var remain = int32(len(allColumn))

	for _, i := range allColumn {
		wg.Add(1)
		guard <- struct{}{}
		go func(i UUIDColumn) {
			getRecord(i.column, i.schema, i.table, &wg, &remain)
			<-guard
		}(i)
	}
	wg.Wait()

	for _, i := range allColumn {
		renameKey(i.column, i.schema, i.table)
	}
}

