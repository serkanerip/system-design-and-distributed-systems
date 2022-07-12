package databaseexperiment

import (
	"database-experiment/config"
	"fmt"
	"github.com/google/uuid"
	"time"
)

func getFileAbsolutePath(fileName string) string {
	return fmt.Sprintf("%s/%s", config.DataFilesFolderPath, fileName)
}

func generateDataFileName() string {
	return fmt.Sprintf("%d-%s.data", time.Now().UnixNano(), uuid.New())
}
