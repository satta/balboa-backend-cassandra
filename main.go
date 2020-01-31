package main

import (
	log "github.com/sirupsen/logrus"
)

func main() {
	h := &handler.CassandraHandler{
	}
	backend.Serve("0.0.0.0:4242", h)
	log.Info("shutdown complete")
}
