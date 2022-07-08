package databaseexperiment

import (
	"bufio"
	"database-experiment/index"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"regexp"
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
	Read(key string) (string, error)
	Write(key, value string)
	GetFileInfo() os.FileInfo
	RecoverIndex()
	Close() error
}

type segment struct {
	readFile      *os.File
	indexStrategy index.Index
	regexForAll   *regexp.Regexp
	regexPattern  string
	header        SegmentHeader
}

func (s *segment) Close() error {
	return s.Close()
}

func (s *segment) RecoverIndex() {
	start := time.Now()
	_, err := s.readFile.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(s.readFile)
	lineCount := 0
	for sc.Scan() {
		res := s.regexForAll.FindAllSubmatch(sc.Bytes(), -1)
		for i := range res {
			s.indexStrategy.Set(string(res[i][1]), string(res[i][3]))
		}
		lineCount++
	}
	fmt.Printf("Recovered segment! LineCount: %d, Time: %dms\n", lineCount, time.Now().Sub(start).Milliseconds())
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

func newSegment(file *os.File, indexStrategy index.Index) *segment {
	return &segment{
		readFile:      file,
		indexStrategy: indexStrategy,
		regexForAll:   regexp.MustCompile(`(?m)^([a-zA-Z\-0-9]+),(.+),([\da-g]+)$`),
		regexPattern:  `(?m)^($key$),(.+),([\da-g]+)$`,
	}
}

type immutableSegment struct {
	segment
}

func (r *immutableSegment) Write(string, string) {
	panic("Immutable segment doesn't support writes!")
}

func (r *immutableSegment) Read(key string) (string, error) {
	regex := regexp.MustCompile(strings.ReplaceAll(r.regexPattern, "$key$", key))
	offset, err := r.indexStrategy.Get(key)
	if err != nil {
		return "", err
	}
	line := r.readLineAtOffset(offset)
	res := regex.FindAllSubmatch(line, -1)
	if len(res) < 1 {
		return "", nil
	}
	return string(res[len(res)-1][2]), nil
}

func (s *segment) readLineAtOffset(offsetStr string) []byte {
	offset, _ := strconv.ParseInt(offsetStr, 16, 0)
	sc := bufio.NewScanner(s.readFile)
	_, err := s.readFile.Seek(offset, 0)
	if err != nil {
		log.Fatal(err)
	}
	sc.Scan()
	return sc.Bytes()
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
	s := &immutableSegment{*newSegment(file, indexStrategy)}
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
		segment: *newSegment(w.readFile, w.indexStrategy),
	}
}

func (s *segment) GetFileInfo() os.FileInfo {
	fInfo, err := s.readFile.Stat()
	if err != nil {
		panic(err)
	}
	return fInfo
}

func (w *writableSegment) Write(key, value string) {
	w.wLock.Lock()
	defer w.wLock.Unlock()
	offset, err := w.wFile.Seek(0, 2)
	if err != nil {
		log.Fatal(err)
	}
	offsetStr := strconv.FormatInt(offset, 16)
	_, err = w.wFile.Write([]byte(fmt.Sprintf("%s,%s,%s\n", key, value, offsetStr)))
	if err != nil {
		log.Fatal(err)
	}
	w.indexStrategy.Set(key, offsetStr)
}

func (w *writableSegment) Read(key string) (string, error) {
	regex := regexp.MustCompile(strings.ReplaceAll(w.regexPattern, "$key$", key))
	offset, err := w.indexStrategy.Get(key)
	if err != nil {
		return "", err
	}
	w.rLock.Lock()
	line := w.readLineAtOffset(offset)
	w.rLock.Unlock()
	res := regex.FindAllSubmatch(line, -1)
	if len(res) < 1 {
		return "", nil
	}
	return string(res[len(res)-1][2]), nil
}

func NewWritableSegment(filePath string, indexStrategy index.Index) *writableSegment {
	file, fileErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, fs.ModePerm)
	if fileErr != nil {
		panic(fileErr)
	}
	ws := &writableSegment{
		segment: *newSegment(file, indexStrategy),
	}
	ws.wFile, fileErr = os.OpenFile(filePath, os.O_WRONLY, fs.ModePerm)
	if fileErr != nil {
		panic(fileErr)
	}

	return ws
}
