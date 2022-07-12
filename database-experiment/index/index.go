package index

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Index interface {
	Get(key string) (string, error)
	GetCreationTime(key string) (int64, error)
	Set(key, offset string, creationTime int64)
	AllKeys() []string
	CollectPromMetrics()
}
