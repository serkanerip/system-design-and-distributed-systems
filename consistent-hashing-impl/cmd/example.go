package main

import (
	chi "consistent-hashing-impl"
	"fmt"
)

func main() {
	ring := chi.Ring{}

	ring.AddNode("S1")
	ring.AddNode("S2")
	ring.AddNode("S3")

	fmt.Printf("%+v\n", ring.GetNodes())
	fmt.Println(ring.Get("R1"), ring.Get("R2"))
	ring.AddNode("S4")
	fmt.Printf("%+v\n", ring.GetNodes())
	fmt.Println(ring.Get("R1"), ring.Get("R2"))
}
