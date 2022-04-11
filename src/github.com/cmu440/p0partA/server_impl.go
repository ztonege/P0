// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/cmu440/p0partA/kvstore"
)

type keyValueServer struct {
	// TODO: implement this!
	store               kvstore.KVStore
	port                int
	clientPool          map[string]*client
	registrationChannel chan *client
	removalChannel      chan string
	dropped             int
}

type client struct {
	id     string
	reader *bufio.Reader
	writer *bufio.Writer
	conn   net.Conn
	// FIXME: think of a better name
	channel chan string
}

func (cli client) String() string {
	return fmt.Sprintf("Client: %v", cli.id)
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	// TODO: implement this!
	clientPool := map[string]*client{}
	registrationChannel := make(chan *client)
	removalChannel := make(chan string)
	kvs := &keyValueServer{store: store, clientPool: clientPool, registrationChannel: registrationChannel, removalChannel: removalChannel}
	return kvs
}

// Reference: https://pkg.go.dev/net@go1.17.6
func (kvs *keyValueServer) Start(port int) error {
	// Create a server and listen to port
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		// FIXME: remove print line.
		fmt.Println("Error listening:", err.Error())
		// FIXME: can't use os package.
		os.Exit(1)
	}
	kvs.port = port
	// Client registration and removal handling
	go poolAdminstration(kvs)

	// Concurrently listen to multiple clients
	go handleClients(ln, kvs)

	return err
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	return len(kvs.clientPool)
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return kvs.dropped
}

// Client registration and removal handling
func poolAdminstration(kvs *keyValueServer) {
	for {
		select {
		// Handle client registrations
		case cli := <-kvs.registrationChannel:
			kvs.clientPool[cli.id] = cli
		// Handle client removals
		case id := <-kvs.removalChannel:
			_, ok := kvs.clientPool[id]
			if !ok {
				fmt.Printf("%v does not exist in clientPool!", id)
			}
			delete(kvs.clientPool, id)
			kvs.dropped++
		}
	}
}

func handleClients(ln net.Listener, kvs *keyValueServer) {
	for {
		// Wait for a connection
		conn, err := ln.Accept()
		if err != nil {
			// FIXME: remove print line.
			fmt.Println("Error accepting: ", err.Error())
			// FIXME: can't use os package
			os.Exit(1)
		}
		go handleConnection(conn, kvs)
	}
}

// Handles incoming requests.
func handleConnection(conn net.Conn, kvs *keyValueServer) {
	// Register client: create client + add to client pool
	cli := registerClient(conn, kvs)
	fmt.Printf("[%v] connected!\n", cli.id)
	// Listen to client
	for {
		// Listen to client
		message, err := cli.reader.ReadString('\n')
		switch err {
		// Handle Get, Put, Update, Delete
		case nil:
			// Send a response back to person contacting us.
			_, writeErr := cli.writer.WriteString("Message Received: " + message)
			if writeErr != nil {
				fmt.Println("Error writing:", writeErr.Error())
			}
			cli.writer.Flush()
			fmt.Printf("[%v] %v", cli.id, message)
		// Check if client closed or terminated connection
		case io.EOF:
			removeClient(cli.id, kvs)
			fmt.Printf("[%v] disconnected\n", cli.id)
			return
		default:
			fmt.Printf("[%v] error\n", cli.id)
		}
	}
}

func registerClient(conn net.Conn, kvs *keyValueServer) *client {
	id := conn.RemoteAddr().String()
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	channel := make(chan string)
	cli := &client{id: id, reader: reader, writer: writer, conn: conn, channel: channel}
	// FIXME: only sends to registration channel now, does not wait for confirmation
	kvs.registrationChannel <- cli
	return cli
}

func removeClient(id string, kvs *keyValueServer) {
	kvs.removalChannel <- id
}
