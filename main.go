package main

import (
	"bufio"
	"distributed-kv-store-go-v2/server"
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
	Peers  []string `json:"peers"`
	Port   string   `json:"port"`
	Leader bool     `json:"leader"`
}

func handleConnection(conn net.Conn, isLeader bool) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading from connection: %s", err)
			return
		}

		args := strings.Split(strings.TrimSpace(msg), " ")
		if len(args) == 0 {
			conn.Write([]byte("ERR: Empty command\n"))
			continue
		}

		operation := args[0]
		if operation == "REPL" {
			server.HandlePeerMessage(conn, args[1:])
			return
		}

		switch operation {
		case "GET":
			store.Get(conn, args)
		case "SET":
			if isLeader {
				store.Set(conn, args)
				server.BroadcastToPeers("REPL " + msg)
			} else {
				conn.Write([]byte("ERR : Not the leader\n"))
			}
		case "DEL":
			if isLeader {
				store.Del(conn, args)
				server.BroadcastToPeers("REPL " + msg)
			} else {
				conn.Write([]byte("ERR : Not the leader\n"))
			}
		case "EXPIRE":
			if isLeader {
				store.Expire(conn, args)
				server.BroadcastToPeers("REPL " + msg)
			} else {
				conn.Write([]byte("ERR : Not the leader\n"))
			}
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

	server.InitReplicator(config.Peers)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go handleConnection(conn, config.Leader)
	}
}
