package store

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	globalCache map[string]interface{}
	ttlMap      map[string]time.Time
	mutex       sync.RWMutex
)

func InitStore() {
	globalCache = make(map[string]interface{})
	ttlMap = make(map[string]time.Time)

	go func() {
		for {
			time.Sleep(1 * time.Second)

			mutex.Lock()
			for k, v := range ttlMap {
				if time.Now().After(v) {
					delete(ttlMap, k)
				}
			}
			mutex.Unlock()
		}
	}()
}

func Get(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("ERR usage: GET <key> \n"))
		return
	}

	key := args[1]

	mutex.RLock()
	value, ok := globalCache[key]
	expiry, exists := ttlMap[key]
	mutex.RUnlock()

	if exists && time.Now().After(expiry) {
		conn.Write([]byte("Key expired \n"))
		mutex.Lock()
		delete(globalCache, key)
		delete(ttlMap, key)
		mutex.Unlock()
		return
	}

	if !ok {
		conn.Write([]byte("Nil\n"))
		return
	}

	conn.Write([]byte(fmt.Sprintf("%v\n", value)))
}

func Set(conn net.Conn, args []string) {
	if len(args) != 3 {
		conn.Write([]byte("ERR usage: SET <key> <value> \n"))
		return
	}

	key := args[1]
	value := args[2]

	mutex.Lock()
	globalCache[key] = value
	mutex.Unlock()

	conn.Write([]byte("OK\n"))
}

func Del(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("ERR usage: DEL <key> \n"))
		return
	}

	key := args[1]

	mutex.Lock()
	delete(globalCache, key)
	mutex.Unlock()

	conn.Write([]byte("OK\n"))
}

func Expire(conn net.Conn, args []string) {
	if len(args) != 3 {
		conn.Write([]byte("ERR usage: EXPIRE <key> <seconds> \n"))
		return
	}

	key := args[1]

	seconds, err := strconv.Atoi(args[2])
	if err != nil {
		conn.Write([]byte("ERR usage: EXPIRE <key> <seconds> \n"))
		return
	}

	mutex.Lock()
	ttlMap[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	mutex.Unlock()

	conn.Write([]byte("OK\n"))
}
