package databaseexperiment

type DBRow struct {
	Key          string
	CreationTime int64
	Offset       string
	Value        interface{}
}
