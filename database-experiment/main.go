package main

import (
	"database-experiment/db"
	"database-experiment/index"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	_ "net/http/pprof"
)

var (
	database *db.Database
)

func main() {
	database = db.NewDatabase(index.NewMemoryIndex())
	database.CollectPromMetrics()
	defer database.Close()
	startHttpServer()
}

func startHttpServer() {
	r := gin.Default()
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
		val := database.Get(key)
		status := 200
		if val == "" {
			status = 404
		}
		c.JSON(status, map[string]string{
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
}
