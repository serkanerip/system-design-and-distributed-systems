package index

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Index interface {
	Get(key string) (string, error)
	Set(key, offset string)
	Recover(key, value string)
	CollectPromMetrics()
}
