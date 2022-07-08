package main

import (
	"bytes"
	"encoding/json"
	"github.com/bxcodec/faker/v3"
	"github.com/google/uuid"
	"log"
	"net/http"
	"sync"
	"time"
)

type Person struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Phone  string `json:"phone"`
	Gender string `json:"gender"`
	Email  string `json:"email"`
}

type CreateKeyValuePair struct {
	Value interface{} `json:"value"`
}

var (
	createdPersons = make(chan Person, 40)
)

func main() {
	go func() {
		for person := range createdPersons {
			time.Sleep(time.Millisecond * 200)
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
	for i := 1; i < 2; i++ {
		wg.Add(1)
		go func() {
			uptimeTicker := time.NewTicker(5 * time.Minute)
			for {
				select {
				case <-uptimeTicker.C:
					wg.Done()
					close(createdPersons)
					return
				default:
					createPerson()
					// time.Sleep(time.Millisecond * 500)
				}
			}
		}()
	}
	wg.Wait()
}

func createPerson() {
	p := Person{
		ID:     uuid.New().String(),
		Name:   faker.Name(),
		Phone:  faker.Phonenumber(),
		Gender: faker.Gender(),
		Email:  faker.Email(),
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
