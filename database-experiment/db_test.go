package databaseexperiment

import (
	"fmt"
	"github.com/bxcodec/faker/v3"
	"github.com/go-playground/assert/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type Person struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Phone  string   `json:"phone"`
	Gender string   `json:"gender"`
	Email  string   `json:"email"`
	X      []string `json:"x"`
}

func TestDb(t *testing.T) {
	db := NewDatabase()
	createdPersons := createSomeData(t, db, 40, 400)

	assert.Equal(t, len(createdPersons), 40*400)
	db.Close()

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(createdPersons), func(i, j int) { createdPersons[i], createdPersons[j] = createdPersons[j], createdPersons[i] })

	assert.Equal(t, nil, validateDataExistence(db, createdPersons))

	db.Close()
	db = NewDatabase()
	assert.Equal(t, nil, validateDataExistence(db, createdPersons))
}

func validateDataExistence(db *Database, dataIds []string) error {
	for i := range dataIds {
		_, err := db.Get(dataIds[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func createSomeData(t *testing.T, db *Database, jobCount, jobPersonCount int) []string {
	l := sync.Mutex{}
	var createdPersons []string
	var wg sync.WaitGroup
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go func() {
			for x := 0; x < jobPersonCount; x++ {
				p := Person{
					ID:     fmt.Sprintf("%s-%d", uuid.New().String(), time.Now().UnixNano()),
					Name:   faker.Name(),
					Phone:  faker.Phonenumber(),
					Gender: faker.Gender(),
					Email:  faker.Email(),
					X:      make([]string, 100_000),
				}
				l.Lock()
				createdPersons = append(createdPersons, p.ID)
				l.Unlock()
				require.Nil(t, db.Set(p.ID, p))
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return createdPersons
}
