package databaseexperiment

import (
	"database-experiment/index"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"time"
)

type databaseFile struct {
	name          string
	createdAt     time.Time
	osFile        *os.File
	indexStrategy index.Index
}

type fileNode struct {
	value databaseFile
	next  *fileNode
}

type filesLinkedList struct {
	len  int
	head *fileNode
	tail *fileNode
}

func (l filesLinkedList) Len() int {
	return l.len
}

// Insert inserts new node at the end of  from linked list
func (l *filesLinkedList) Insert(val databaseFile) {
	n := fileNode{}
	n.value = val
	if l.len == 0 {
		l.head = &n
		l.len++
		return
	}
	ptr := l.head
	for i := 0; i < l.len; i++ {
		if ptr.next == nil {
			ptr.next = &n
			l.len++
			return
		}
		ptr = ptr.next
	}
}

func (l *filesLinkedList) InsertAt(pos int, value databaseFile) {
	// create a new node
	newNode := fileNode{}
	newNode.value = value
	// validate the position
	if pos < 0 {
		return
	}
	if pos == 0 {
		l.head = &newNode
		l.len++
		return
	}
	if pos > l.len {
		return
	}
	n := l.GetAt(pos)
	newNode.next = n
	prevNode := l.GetAt(pos - 1)
	prevNode.next = &newNode
	l.len++
}

// GetAt returns node at given position from linked list
func (l *filesLinkedList) GetAt(pos int) *fileNode {
	ptr := l.head
	if pos < 0 {
		return ptr
	}
	if pos > (l.len - 1) {
		return nil
	}
	for i := 0; i < pos; i++ {
		ptr = ptr.next
	}
	return ptr
}

// DeleteAt deletes node at given position from linked list
func (l *filesLinkedList) DeleteAt(pos int) error {
	// validate the position
	if pos < 0 {
		fmt.Println("position can not be negative")
		return errors.New("position can not be negative")
	}
	if l.len == 0 {
		fmt.Println("No nodes in list")
		return errors.New("no nodes in list")
	}
	prevNode := l.GetAt(pos - 1)
	if prevNode == nil {
		fmt.Println("Node not found")
		return errors.New("node not found")
	}
	prevNode.next = l.GetAt(pos).next
	l.len--
	return nil
}

func newFileNodeFromFileInfo(folderPath string, fileInfo fs.FileInfo) *fileNode {
	file, openFileErr := os.OpenFile(folderPath+"/"+fileInfo.Name(), os.O_RDWR, 0)
	if openFileErr != nil {
		log.Fatalf("Couldn't open database file err is: %v", openFileErr)
	}
	return &fileNode{
		value: databaseFile{
			name:          fileInfo.Name(),
			createdAt:     fileInfo.ModTime(),
			osFile:        file,
			indexStrategy: nil,
		},
		next: nil,
	}
}
