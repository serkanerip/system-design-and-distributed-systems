package index

type Index interface {
	Get(key string) string
	Set(key, offset string)
	Recover(key, value string)
	CollectPromMetrics()
}
