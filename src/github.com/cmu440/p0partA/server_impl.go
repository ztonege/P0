// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/cmu440/p0partA/kvstore"
)

type keyValueServer struct {
	// TODO: implement this!
	store kvstore.KVStore
	port  int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	// TODO: implement this!
	kvs := &keyValueServer{store: store}
	return kvs
}

// Reference: https://pkg.go.dev/net@go1.17.6
func (kvs *keyValueServer) Start(port int) error {
	// Create a server and listen to port
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	kvs.port = port
	go func() {
		for {
			// Wait for a connection
			conn, err := ln.Accept()
			if err != nil {
				// handle error
				fmt.Println("Error accepting: ", err.Error())
				os.Exit(1)
			}
			go handleConnection(conn)
		}
	}()

	return err
}

// Handles incoming requests.
func handleConnection(conn net.Conn) {
	strRemoteAddr := conn.RemoteAddr().String()
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	for {
		message, readErr := reader.ReadString('\n')
		if readErr != nil {
			fmt.Println("Error reading:", readErr.Error())
		}
		// Send a response back to person contacting us.
		_, writeErr := writer.WriteString("Message Received: " + message)
		if writeErr != nil {
			fmt.Println("Error writing:", writeErr.Error())
		}
		writer.Flush()
		fmt.Printf("[%v] %v", strRemoteAddr, message)
	}
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	return -1
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return -1
}

// TODO: add additional methods/functions below!
