package server

import (
	"distributed-kv-store-go-v2/store"
	"log"
	"net"
)

var globalPeers []string

func InitReplicator(peers []string) {
	globalPeers = peers
}

func BroadcastToPeers(command string) {
	for _, peer := range globalPeers {
		conn, err := net.Dial("tcp", peer)
		if err != nil {
			log.Printf("Error connecting to peer %s\n", peer)
			continue
		}
		conn.Write([]byte(command))
	}
}

func HandlePeerMessage(conn net.Conn, args []string) {
	operation := args[0]
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
		conn.Write([]byte("Unknown command\n"))
	}
}
