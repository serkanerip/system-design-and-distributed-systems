package main

import (
	db "database-experiment"
	"database-experiment/index"
	"encoding/json"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"sync"
)

var (
	database *db.Database
	wg       sync.WaitGroup
)

func main() {
	wg.Add(1)
	go startHttpServer()
	database = db.NewDatabase()
	defer database.Close()
	wg.Wait()
}

func startHttpServer() {
	r := gin.Default()

	pprof.Register(r)

	r.GET("/prom_metrics", func(c *gin.Context) {
		h := promhttp.Handler()
		h.ServeHTTP(c.Writer, c.Request)
	})

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, map[string]string{
			"status": "ok",
		})
	})

	r.GET("/db/:key", func(c *gin.Context) {
		key := c.Param("key")
		val, err := database.Get(key)
		status := 200
		if err != nil {
			status = 500
			if err == index.ErrKeyNotFound {
				status = 404
			}
		}

		c.JSON(status, map[string]interface{}{
			"value": val,
		})
	})

	r.POST("/db/:key", func(c *gin.Context) {
		key := c.Param("key")
		var body map[string]interface{}
		if err := c.BindJSON(&body); err != nil {
			log.Printf("Couldn't bind request body err is: %v\n", err)
			c.Status(500)
			return
		}
		marshal, err := json.Marshal(body["value"])
		if err != nil {
			panic(err)
		}
		database.Set(key, string(marshal))
		c.Status(200)
	})

	log.Fatal(r.Run(":3000"))
	wg.Done()
}
