package databaseexperiment

import (
	"database-experiment/index"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"
)

type Database struct {
	currentSegment       *writableSegment
	segmentSizeThreshold int64
	dataFilesFolderPath  string
	frozenSegments       []Segment
	segmentLock          sync.Mutex
}

func NewDatabase() *Database {
	db := &Database{
		dataFilesFolderPath:  "/Users/serkanerip/workspace/tmp",
		segmentSizeThreshold: 1_000_000,
		frozenSegments:       []Segment{},
	}
	db.findSegments()
	db.currentSegment = NewWritableSegment(
		db.getFileAbsolutePath(db.generateDataFileName()),
		index.NewHashMapIndex(),
	)
	db.Recover()

	ticker := time.NewTicker(time.Minute * 1)
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
	db.currentSegment.Close()
}

func (db *Database) Get(key string) (string, error) {
	data, err := db.currentSegment.Read(key)
	if err == nil {
		return data, nil
	}
	if err != index.ErrKeyNotFound {
		return "", err
	}
	for i := range db.frozenSegments {
		data, err = db.frozenSegments[i].Read(key)
		if err == nil {
			return data, nil
		}
		if err != index.ErrKeyNotFound {
			return "", err
		}
	}
	return "", index.ErrKeyNotFound
}

func (db *Database) Set(key, value string) {
	db.currentSegment.Write(key, value)
	go db.checkCurrentSegmentSize()
}

func (db *Database) Recover() {
	db.currentSegment.RecoverIndex()
	for i := range db.frozenSegments {
		db.frozenSegments[i].RecoverIndex()
	}
}

func (db *Database) compaction() {
	fmt.Println("compaction")
}

func (db *Database) checkCurrentSegmentSize() {
	fileInfo := db.currentSegment.GetFileInfo()
	currentSegmentSize := fileInfo.Size()
	if currentSegmentSize < db.segmentSizeThreshold {
		return
	}
	fmt.Println("Froze current segment it exceeded the threshold! Size: ", currentSegmentSize)
	db.initNewWritableSegment()
}

func (db *Database) initNewWritableSegment() {
	if !db.segmentLock.TryLock() {
		return // another process is doing check at the moment
	}
	fileName := db.generateDataFileName()
	oldSegment := db.currentSegment
	db.currentSegment = NewWritableSegment(
		db.getFileAbsolutePath(fileName),
		index.NewHashMapIndex())
	db.frozenSegments = append(db.frozenSegments, oldSegment.getImmutableSegment())
	fmt.Println("Frozed old segment and new segment created!")
	db.segmentLock.Unlock()
}

func (db *Database) generateDataFileName() string {
	return fmt.Sprintf("%d-%s.data", time.Now().UnixNano(), uuid.New())
}

func (db *Database) SegmentsInfo() map[string]interface{} {
	var frozens []string
	for i := range db.frozenSegments {
		frozens = append(frozens, db.frozenSegments[i].GetFileInfo().Name())
	}
	return map[string]interface{}{
		"current":  db.currentSegment.GetFileInfo().Name(),
		"fronzens": frozens,
	}
}

func (db *Database) findSegments() {
	fileInfos, err := ioutil.ReadDir(db.dataFilesFolderPath)
	if err != nil {
		panic(err)
		return
	}

	for _, fileInfo := range fileInfos {
		fileName := fileInfo.Name()
		absolutePath := db.getFileAbsolutePath(fileName)
		if strings.Contains(fileName, ".data") {
			db.frozenSegments = append(db.frozenSegments, NewImmutableSegment(absolutePath, index.NewHashMapIndex()))
		}
	}

	sort.Slice(db.frozenSegments, func(i, j int) bool {
		return db.frozenSegments[i].GetFileInfo().Name() > db.frozenSegments[j].GetFileInfo().Name()
	})
}

func (db *Database) getFileAbsolutePath(fileName string) string {
	return fmt.Sprintf("%s/%s", db.dataFilesFolderPath, fileName)
}
