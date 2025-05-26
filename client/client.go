package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	fmt.Println("Client starting...")

	conn, err := net.Dial("tcp", os.Args[1])
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println("Client connected!")
	defer conn.Close()

	go func() {
		reader := bufio.NewReader(conn)
		for {
			msg, err := reader.ReadString('\n')
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(msg)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if scanner.Scan() {
			text := scanner.Text() + "\n"
			_, err := conn.Write([]byte(text))
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
