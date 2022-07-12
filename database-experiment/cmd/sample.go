package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bxcodec/faker/v3"
	"github.com/google/uuid"
	"log"
	"net/http"
	"sync"
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

type CreateKeyValuePair struct {
	Value interface{} `json:"value"`
}

var (
	createdPersons = make(chan Person, 1000)
)

func main() {
	go func() {
		for person := range createdPersons {
			time.Sleep(time.Millisecond * 100)
			get, err := http.Get("http://localhost:3000/db/" + person.ID)
			if err != nil {
				panic(err)
			}
			if get.StatusCode == http.StatusNotFound {
				log.Printf("Response for %s key is 404!\n", person.ID)
			}
			get.Body.Close()
		}
	}()

	var wg sync.WaitGroup
	uptimeTicker := time.NewTicker(30 * time.Minute)
	for i := 1; i < 50; i++ {
		wg.Add(1)
		go func() {
			for {
				select {
				case <-uptimeTicker.C:
					wg.Done()
					close(createdPersons)
					return
				default:
					createPerson()
					time.Sleep(time.Millisecond * 200)
				}
			}
		}()
	}
	wg.Wait()
}

func createPerson() {
	p := Person{
		ID:     fmt.Sprintf("%s-%d", uuid.New().String(), time.Now().UnixNano()),
		Name:   faker.Name(),
		Phone:  faker.Phonenumber(),
		Gender: faker.Gender(),
		Email:  faker.Email(),
		X:      make([]string, 100_000),
	}

	b, err := json.Marshal(CreateKeyValuePair{Value: p})
	if err != nil {
		panic(err)
	}
	reader := bytes.NewReader(b)
	postResp, postErr := http.Post("http://localhost:3000/db/"+p.ID, "application/json", reader)
	if postErr != nil {
		panic(postErr)
	}
	defer postResp.Body.Close()
	// createdPersons <- p
}
