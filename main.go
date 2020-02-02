package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/satta/balboa-backend-cassandra/handler"

	backend "github.com/DCSO/balboa/backend/go"
)

func main() {
	h, err := handler.MakeCassandraHandler([]string{"localhost"}, "", "", 1)
	if err != nil {
		log.Fatal(err)
	}
	backend.Serve("0.0.0.0:4242", h)
	log.Info("shutdown complete")
}
