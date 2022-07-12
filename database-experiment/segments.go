package databaseexperiment

import (
	"database-experiment/index"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"io/fs"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	headerLength = 1024
	magicNumber  = 218
)

type SegmentHeader struct {
	IsCompacted bool
	Checksum    [256]byte
}

type Segment interface {
	Read(key string) (interface{}, error)
	Write(key string, value interface{}) error
	GetFileInfo() os.FileInfo
	RecoverIndex()
	GetUniqueKeys() []string
	GetIndexStrategy() index.Index
	GetId() string
	Close() error
}

type segment struct {
	id            string
	readFile      *os.File
	indexStrategy index.Index
	header        SegmentHeader
}

func (s *segment) Close() error {
	return s.readFile.Close()
}

func (s *segment) GetIndexStrategy() index.Index {
	return s.indexStrategy
}

func (s *segment) GetUniqueKeys() []string {
	return s.indexStrategy.AllKeys()
}

func (s *segment) RecoverIndex() {
	start := time.Now()
	fmt.Println("Started to recover segment: ", s.id)
	_, err := s.readFile.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	lineCount := 0
	for {
		rowLenBytes := make([]byte, 8)
		readCount, readErr := s.readFile.Read(rowLenBytes)
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			panic(err)
		}
		if readCount != 8 {
			fmt.Println("Empty segment!")
			return
		}
		recordLen := binary.LittleEndian.Uint64(rowLenBytes)
		recordBytes := make([]byte, recordLen)

		_, readErr = s.readFile.Read(recordBytes)
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			panic(readErr)
		}

		s.indexTheLine(s.id, recordBytes)
		lineCount++
	}
	fmt.Printf("Recovered segment! LineCount: %d, Time: %dms\n", lineCount, time.Now().Sub(start).Milliseconds())
}

func (s *segment) GetId() string {
	return s.id
}

func (s *segment) indexTheLine(sid string, line []byte) {
	var row DBRow
	unmarshalErr := msgpack.Unmarshal(line, &row)
	if unmarshalErr != nil {
		fmt.Println("couldn't unmarshal the row", unmarshalErr, line, sid)
		panic(string(line))
	}
	if row.Key == "" {
		fmt.Println("data is corrupted!")
		return
	}
	s.indexStrategy.Set(row.Key, row.Offset, row.CreationTime)
}

func (s *segment) setSegmentHeader() {
	b := make([]byte, headerLength, headerLength)
	readCount, err := s.readFile.Read(b)
	if err != nil {
		panic(err)
	}
	if readCount != headerLength {
		panic("read count is lower than header length")
	}

	if b[0] != magicNumber {
		panic("wrong magic number!")
	}

	h := SegmentHeader{}

	if b[1]&1 != 1 {
		h.IsCompacted = true
	}
}

func newSegment(id string, file *os.File, indexStrategy index.Index) *segment {
	return &segment{
		id:            id,
		readFile:      file,
		indexStrategy: indexStrategy,
	}
}

type immutableSegment struct {
	segment
}

func (r *immutableSegment) Write(string, interface{}) error {
	panic("Immutable segment doesn't support writes!")
}

func (r *immutableSegment) Read(key string) (interface{}, error) {
	offset, err := r.indexStrategy.Get(key)
	if err != nil {
		return "", err
	}
	recordBytes := r.readRecordAtOffset(offset)
	var row DBRow
	unmarshalErr := msgpack.Unmarshal(recordBytes, &row)
	if unmarshalErr != nil {
		return nil, err
	}
	if row.Key != key {
		return "", errors.New("data is corrupted")
	}
	return row.Value, nil
}

func (s *segment) readRecordAtOffset(offsetStr string) []byte {
	offset, _ := strconv.ParseInt(offsetStr, 16, 0)
	_, err := s.readFile.Seek(offset, 0)
	if err != nil {
		log.Fatal(err)
	}
	rowLenBytes := make([]byte, 8)
	_, readErr := s.readFile.Read(rowLenBytes)
	if readErr != nil {
		panic(err)
	}
	recordLen := binary.LittleEndian.Uint64(rowLenBytes)
	recordBytes := make([]byte, recordLen)

	_, readErr = s.readFile.Read(recordBytes)
	if readErr != nil {
		panic(readErr)
	}

	return recordBytes
}

func NewImmutableSegment(filePath string, indexStrategy index.Index) Segment {
	isNewFile := false
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		isNewFile = true
	}
	file, fileErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, fs.ModePerm)
	if fileErr != nil {
		panic(fileErr)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil
	}
	s := &immutableSegment{*newSegment(stat.Name(), file, indexStrategy)}
	if isNewFile {
		// s.setSegmentHeader()
	}

	return s
}

type writableSegment struct {
	wLock sync.Mutex
	segment
	rLock sync.Mutex
	wFile *os.File
}

func (w *writableSegment) getImmutableSegment() *immutableSegment {
	return &immutableSegment{
		segment: *newSegment(w.id, w.readFile, w.indexStrategy),
	}
}

func (s *segment) GetFileInfo() os.FileInfo {
	fInfo, err := s.readFile.Stat()
	if err != nil {
		panic(err)
	}
	return fInfo
}

func (w *writableSegment) Write(key string, value interface{}) error {
	w.wLock.Lock()
	defer w.wLock.Unlock()
	offset, err := w.wFile.Seek(0, 2)
	if err != nil {
		return err
	}
	offsetStr := strconv.FormatInt(offset, 16)
	dbRow := DBRow{
		Key:          key,
		CreationTime: time.Now().Unix(),
		Value:        value,
		Offset:       offsetStr,
	}
	rowBytes, err := msgpack.Marshal(&dbRow)
	if err != nil {
		return err
	}
	rowLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(rowLength, uint64(len(rowBytes)))
	_, err = w.wFile.Write(append(rowLength, rowBytes...))
	if err != nil {
		return err
	}
	w.indexStrategy.Set(key, offsetStr, dbRow.CreationTime)
	return nil
}

func (w *writableSegment) Read(key string) (interface{}, error) {
	offset, err := w.indexStrategy.Get(key)
	if err != nil {
		return "", err
	}
	w.rLock.Lock()
	line := w.readRecordAtOffset(offset)
	w.rLock.Unlock()
	var row DBRow
	unmarshalErr := msgpack.Unmarshal(line, &row)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if row.Key != key {
		return "", errors.New("data is corrupted")
	}
	return row.Value, nil
}

func (w *writableSegment) Close() error {
	w.readFile.Close()
	w.wFile.Close()
	return nil
}

func NewWritableSegment(filePath string, indexStrategy index.Index) *writableSegment {
	file, fileErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, fs.ModePerm)
	if fileErr != nil {
		panic(fileErr)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil
	}
	ws := &writableSegment{
		segment: *newSegment(stat.Name(), file, indexStrategy),
	}
	ws.wFile, fileErr = os.OpenFile(filePath, os.O_WRONLY, fs.ModePerm)
	if fileErr != nil {
		panic(fileErr)
	}

	return ws
}

var (
	errKeyIsNotInSegments = errors.New("couldn't find the key inside segments")
)

type Segments struct {
	segmentsLock sync.Mutex
	segments     []Segment
	sync.Mutex
	compactionInProgress bool
	mergeInProgress      bool
}

func (s *Segments) IsCompactionInProgress() bool {
	return s.compactionInProgress
}

func (s *Segments) Add(seg Segment) {
	s.segmentsLock.Lock()
	defer s.segmentsLock.Unlock()
	s.segments = append(s.segments, seg)
}

func (s *Segments) Delete(id string) error {
	s.segmentsLock.Lock()
	defer s.segmentsLock.Unlock()
	segmentIndex := -1
	for i := range s.segments {
		if s.segments[i].GetId() == id {
			segmentIndex = i
			break
		}
	}
	if segmentIndex == -1 {
		return errors.New("couldn't find segment to delete")
	}

	s.segments[segmentIndex] = nil
	s.segments = append(s.segments[:segmentIndex], s.segments[segmentIndex+1:]...)
	return nil
}

func (s *Segments) Recover() {
	var wg sync.WaitGroup
	for i := range s.segments {
		wg.Add(1)
		go func(index int) {
			s.segments[index].RecoverIndex()
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("Segments recovering is done!")
}

func (s *Segments) Sort() {
	sort.Slice(s.segments, func(i, j int) bool {
		return s.segments[i].GetFileInfo().Name() < s.segments[j].GetFileInfo().Name()
	})
}

func (s *Segments) FindKeyInsideSegments(key string) (interface{}, error) {
	for i := len(s.segments) - 1; i > -1; i-- {
		data, err := s.segments[i].Read(key)
		if err == nil {
			return data, nil
		}
		if err != index.ErrKeyNotFound {
			log.Println("Segment read error: ", err)
			return "", err
		}
	}
	return nil, errKeyIsNotInSegments
}

func (s *Segments) Merge() {
	s.Lock()
	defer s.Unlock()
	if len(s.segments) < 2 {
		return
	}
	s.mergeInProgress = true
	fmt.Println("Started to Merge")
	startTime := time.Now()
	var compactedSegments []Segment
	for i := range s.segments {
		if strings.Contains(s.segments[i].GetId(), ".compact") {
			compactedSegments = append(compactedSegments, s.segments[i])
		}
	}

	if len(compactedSegments) < 1 {
		s.mergeInProgress = false
		fmt.Printf("Merge done in %f seconds.\n", time.Now().Sub(startTime).Seconds())
	}

	newSegment := NewWritableSegment(getFileAbsolutePath(generateDataFileName()), index.NewHashMapIndex())
	for i := range compactedSegments {
		keys := compactedSegments[i].GetUniqueKeys()
		for _, key := range keys {
			val, readErr := compactedSegments[i].Read(key)
			if readErr != nil {
				panic(readErr)
			}
			err := newSegment.Write(key, val)
			if err != nil {
				panic(err)
			}
		}
	}
	s.Add(newSegment.getImmutableSegment())
	for i := range compactedSegments {
		err := s.Delete(compactedSegments[i].GetId())
		if err != nil {
			panic(err)
		}
		removeErr := os.Remove(getFileAbsolutePath(compactedSegments[i].GetId()))
		if removeErr != nil {
			panic(removeErr)
		}
	}
	s.mergeInProgress = false
	fmt.Printf("Merge done in %f seconds.\n", time.Now().Sub(startTime).Seconds())
}

func (s *Segments) Compaction() {
	s.Lock()
	defer s.Unlock()
	fmt.Println("Started to Compaction")
	startTime := time.Now()
	s.compactionInProgress = true

	var segmentsThatNeedCompaction []Segment
	for i := range s.segments {
		if !strings.Contains(s.segments[i].GetId(), ".compact") {
			segmentsThatNeedCompaction = append(segmentsThatNeedCompaction, s.segments[i])
		}
	}

	var wg sync.WaitGroup
	for _, seg := range segmentsThatNeedCompaction {
		wg.Add(1)
		go func(safeSegment Segment) {
			newSegment := NewWritableSegment(getFileAbsolutePath(safeSegment.GetId()+".compact"), safeSegment.GetIndexStrategy())
			for _, key := range newSegment.GetUniqueKeys() {
				val, readErr := safeSegment.Read(key)
				if readErr != nil {
					panic(readErr)
				}
				writeErr := newSegment.Write(key, val)
				if writeErr != nil {
					panic(writeErr)
				}
			}
			deleteErr := s.Delete(safeSegment.GetId())
			if deleteErr != nil {
				panic(deleteErr)
			}
			removeErr := os.Remove(getFileAbsolutePath(safeSegment.GetId()))
			if removeErr != nil {
				panic(removeErr)
			}
			s.Add(newSegment.getImmutableSegment())
			wg.Done()
		}(seg)
	}
	wg.Wait()
	s.compactionInProgress = false
	fmt.Printf("Compaction done in %f seconds.\n", time.Now().Sub(startTime).Seconds())
}

func NewSegments() *Segments {
	return &Segments{segments: []Segment{}}
}
