package db

import (
	"bufio"
	"database-experiment/index"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Database struct {
	regexForAll   *regexp.Regexp
	regexPattern  string
	indexStrategy index.Index
	l             sync.Mutex
	file          *os.File
}

func NewDatabase(indexStrategy index.Index) *Database {
	db := &Database{
		regexForAll:   regexp.MustCompile(`(?m)^([a-zA-Z\-0-9]+),(.+),([\da-g]+)$`),
		regexPattern:  `(?m)^($key$),(.+),([\da-g]+)$`,
		indexStrategy: indexStrategy,
	}
	var fileErr error
	db.file, fileErr = os.OpenFile("db.data", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0)
	if fileErr != nil {
		panic(fileErr)
	}
	db.Recover()

	ticker := time.NewTicker(time.Minute * 5)
	go func() {
		for {
			select {
			case <-ticker.C:
				db.compaction()
			}
		}
	}()

	return db
}

func (db *Database) Close() {
	db.file.Close()
}

func (db *Database) CollectPromMetrics() {
	db.indexStrategy.CollectPromMetrics()
}

func (db *Database) Get(key string) string {
	regex := regexp.MustCompile(strings.ReplaceAll(db.regexPattern, "$key$", key))
	offset := db.indexStrategy.Get(key)
	line := db.getLineFromOffset(offset)
	res := regex.FindAllSubmatch(line, -1)
	if len(res) < 1 {
		return ""
	}
	return string(res[len(res)-1][2])
}

func (db *Database) Set(key, value string) {
	db.l.Lock()
	offset, err := db.file.Seek(0, 2)
	if err != nil {
		log.Fatal(err)
	}
	offsetStr := strconv.FormatInt(offset, 16)
	_, err = db.file.Write([]byte(fmt.Sprintf("%s,%s,%s\n", key, value, offsetStr)))
	if err != nil {
		log.Fatal(err)
	}
	db.indexStrategy.Set(key, offsetStr)
	db.l.Unlock()
}

func (db *Database) Recover() {
	start := time.Now()
	_, err := db.file.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(db.file)
	lineCount := 0
	for sc.Scan() {
		res := db.regexForAll.FindAllSubmatch(sc.Bytes(), -1)
		for i := range res {
			db.indexStrategy.Set(string(res[i][1]), string(res[i][3]))
		}
		lineCount++
	}
	fmt.Println(time.Now().Sub(start).Milliseconds(), lineCount)
}

func (db *Database) compaction() {
	fmt.Println("compaction")
}

func (db *Database) getLineFromOffset(offsetStr string) []byte {
	offset, _ := strconv.ParseInt(offsetStr, 16, 0)
	sc := bufio.NewScanner(db.file)
	db.l.Lock()
	_, err := db.file.Seek(offset, 0)
	if err != nil {
		log.Fatal(err)
	}
	sc.Scan()
	db.l.Unlock()
	return sc.Bytes()
}
