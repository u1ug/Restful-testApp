package main

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"log"
	"net"
	"net/http"
	"rest/internal/user"
	"rest/pkg/logging"
	"time"
)

func IndexHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	w.Write([]byte(fmt.Sprintf("Hello %s", name)))
}

func main() {
	logger := logging.GetLogger()
	logger.Info("create router")
	router := httprouter.New()

	log.Println("register user handler")
	handler := user.NewHandler(logger)
	handler.Register(router)

	start(router)
}

func start(router *httprouter.Router) {
	logger := logging.GetLogger()
	logger.Info("starting")
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	server := &http.Server{
		Handler:      router,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	logger.Info("listening on :8080")
	logger.Fatal(server.Serve(listener))
}
