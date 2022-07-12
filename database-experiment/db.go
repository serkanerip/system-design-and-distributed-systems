package databaseexperiment

import (
	"database-experiment/config"
	"database-experiment/index"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

const (
	tombstoneValue = "$__TOMBSTONE__$"
)

type Database struct {
	currentSegment                   *writableSegment
	segmentSizeThreshold             int64
	frozenSegments                   *Segments
	segmentLock                      sync.Mutex
	newWritableSegmentInitInProgress bool
}

func NewDatabase() *Database {
	db := &Database{
		segmentSizeThreshold:             32_000_000,
		frozenSegments:                   NewSegments(),
		newWritableSegmentInitInProgress: false,
	}
	db.findSegments()
	db.frozenSegments.Recover()
	db.frozenSegments.Compaction()
	db.frozenSegments.Merge()
	db.currentSegment = NewWritableSegment(
		getFileAbsolutePath(generateDataFileName()),
		index.NewHashMapIndex(),
	)

	ticker := time.NewTicker(time.Minute * 5)
	go func() {
		for {
			select {
			case <-ticker.C:
				if !db.frozenSegments.compactionInProgress {
					db.frozenSegments.Compaction()
				}
				if !db.frozenSegments.mergeInProgress {
					db.frozenSegments.Merge()
				}
			default:
			}
		}
	}()

	fmt.Println("Database is ready!")
	return db
}

func (db *Database) Close() {
	err := db.currentSegment.Close()
	if err != nil {
		panic(err)
	}
}

func (db *Database) Get(key string) (interface{}, error) {
	pmTotalReads.Inc()
	data, err := db.currentSegment.Read(key)
	if err == nil {
		if data.(string) == tombstoneValue {
			return nil, index.ErrKeyNotFound
		}
		return data, nil
	}
	if err != index.ErrKeyNotFound {
		return nil, err
	}
	data, err = db.frozenSegments.FindKeyInsideSegments(key)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (db *Database) Set(key string, value interface{}) error {
	pmTotalWrites.Inc()
	err := db.currentSegment.Write(key, value)
	if err != nil {
		fmt.Printf("couldn't set key %s err is: %v\n", key, err)
		return err
	}
	go db.checkCurrentSegmentSize()
	return nil
}

func (db *Database) checkCurrentSegmentSize() {
	fileInfo := db.currentSegment.GetFileInfo()
	currentSegmentSize := fileInfo.Size()
	if currentSegmentSize < db.segmentSizeThreshold ||
		db.frozenSegments.IsCompactionInProgress() ||
		db.newWritableSegmentInitInProgress {
		return
	}
	db.newWritableSegmentInitInProgress = true
	fmt.Println("Froze current segment it exceeded the threshold! Size: ", currentSegmentSize)
	db.initNewWritableSegment()
	db.newWritableSegmentInitInProgress = false
}

func (db *Database) initNewWritableSegment() {
	if !db.segmentLock.TryLock() {
		return // another process is doing check at the moment
	}
	fileName := generateDataFileName()
	oldSegment := db.currentSegment
	db.currentSegment = NewWritableSegment(
		getFileAbsolutePath(fileName),
		index.NewHashMapIndex())
	db.segmentLock.Unlock()
	db.frozenSegments.Add(oldSegment.getImmutableSegment())
	fmt.Println("Frozed old segment and new segment created!")
}

func (db *Database) findSegments() {
	fileInfos, err := ioutil.ReadDir(config.DataFilesFolderPath)
	if err != nil {
		panic(err)
		return
	}

	for _, fileInfo := range fileInfos {
		fileName := fileInfo.Name()
		absolutePath := getFileAbsolutePath(fileName)
		if strings.Contains(fileName, ".data") {
			db.frozenSegments.Add(NewImmutableSegment(absolutePath, index.NewHashMapIndex()))
		}
	}

	db.frozenSegments.Sort()
}

func (db *Database) Delete(s string) error {
	return db.Set(s, tombstoneValue)
}
