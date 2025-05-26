package main

import (
	"bufio"
	"distributed-kv-store-go-v2/store"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
)

type Config struct {
	Peers []string `json:"peers"`
	Port  string   `json:"port"`
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading from connection: %s", err)
			return
		}

		args := strings.Split(strings.TrimSpace(msg), " ")

		operation := args[1]
		if operation == "REPL" {
			// handle replication
		}

		switch operation {
		case "GET":
			store.Get(conn, args)
		case "SET":
			store.Set(conn, args)
		case "DEL":
			store.Del(conn, args)
		case "EXPIRE":
			store.Expire(conn, args)
		default:
			conn.Write([]byte(fmt.Sprintf("Unknown operation: %v \n" + operation)))
		}

	}
}

func main() {
	fmt.Println("Node starting....")

	store.InitStore()

	jsonFile, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	bytesRead, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	json.Unmarshal(bytesRead, &config)

	ln, err := net.Listen("tcp", config.Port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go handleConnection(conn)
	}
}
