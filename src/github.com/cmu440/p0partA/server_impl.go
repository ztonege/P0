// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/cmu440/p0partA/kvstore"
)

type ActionOperation int

const (
	Get ActionOperation = iota
	Put
	Update
	Delete
	Undefined
)

func ToActionOperation(opString string) (ActionOperation, error) {
	var op ActionOperation
	switch opString {
	case "Get":
		op = Get
	case "Put":
		op = Put
	case "Update":
		op = Update
	case "Delete":
		op = Delete
	default:
		return Undefined, fmt.Errorf("ActionOperation undefined!")
	}
	return op, nil
}

func (op ActionOperation) String() string {
	switch op {
	case Get:
		return "Get"
	case Put:
		return "Put"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	default:
		return ""
	}
}

type CountOperation int

const (
	Active CountOperation = iota
	Dropped
)

type keyValueServer struct {
	store               kvstore.KVStore
	clientPool          map[string]*client
	registrationChannel chan *client
	removalChannel      chan string
	dropped             int
	countChannel        chan count
	actionChannel       chan action
}

type count struct {
	op    CountOperation
	value int
}

type action struct {
	op     ActionOperation
	key    string
	values [][]byte
	cli    *client
}

type client struct {
	id     string
	reader *bufio.Reader
	writer *bufio.Writer
	conn   net.Conn
	// FIXME: think of a better name
	channel chan [][]byte
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
	actionChannel := make(chan action)
	countChannel := make(chan count)
	kvs := &keyValueServer{store: store, clientPool: clientPool, registrationChannel: registrationChannel, removalChannel: removalChannel, actionChannel: actionChannel, countChannel: countChannel}
	return kvs
}

// Reference: https://pkg.go.dev/net@go1.17.6
func (kvs *keyValueServer) Start(port int) error {
	// Create a server and listen to port
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		// FIXME: remove print line.
		fmt.Println("Error listening:", err.Error())
		return err
	}
	// Routine for handling client registration and removal
	go handlePoolAdminstration(kvs)

	// Routine for handling kv actions
	go handleKvActions(kvs)

	// Routine for concurrently listening on multiple clients
	go handleClients(ln, kvs)

	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// FIXME:
	// 1. Same channel used for both request/response: do we want to separate these two?
	// 2. Cannot identify if it is active or dropped count. Will not work if there are multiple routines asking for counts.
	kvs.countChannel <- count{op: Active}
	c := <-kvs.countChannel
	return c.value
}

func (kvs *keyValueServer) CountDropped() int {
	// FIXME:
	// 1. Same channel used for both request/response: do we want to separate these two?
	// 2. Cannot identify if it is active or dropped count. Will not work if there are multiple routines asking for counts.
	kvs.countChannel <- count{op: Dropped}
	c := <-kvs.countChannel
	return c.value
}

// Client registration and removal handling
func handlePoolAdminstration(kvs *keyValueServer) {
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
		case c := <-kvs.countChannel:
			switch c.op {
			case Active:
				kvs.countChannel <- count{value: len(kvs.clientPool)}
			case Dropped:
				kvs.countChannel <- count{value: kvs.dropped}
			}
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

func handleKvActions(kvs *keyValueServer) {
	for act := range kvs.actionChannel {
		// TODO: handle failures
		switch act.op {
		case Get:
			value := kvs.store.Get(act.key)
			act.cli.channel <- value
		case Put:
			kvs.store.Put(act.key, act.values[0])
		case Update:
			kvs.store.Update(act.key, act.values[0], act.values[1])
		case Delete:
			kvs.store.Delete(act.key)
		}
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
			// Parse the message into actions
			act := messageToAction(message, cli)
			// Send to actionChannel to handle action
			kvs.actionChannel <- act

			// If Get, need to send responses to client
			if act.op == Get {
				values := <-cli.channel
				// Send responses
				for _, value := range values {
					response := fmt.Sprintf("%v:%v\n", act.key, string(value))
					_, writeErr := cli.writer.WriteString(response)
					if writeErr != nil {
						fmt.Println("Error writing:", writeErr.Error())
					}
					cli.writer.Flush()
				}
			}
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
	channel := make(chan [][]byte)
	cli := &client{id: id, reader: reader, writer: writer, conn: conn, channel: channel}
	// FIXME: only sends to registration channel now, does not wait for confirmation
	kvs.registrationChannel <- cli
	return cli
}

func removeClient(id string, kvs *keyValueServer) {
	kvs.removalChannel <- id
}

func messageToAction(message string, cli *client) action {
	// Parse the message into actions
	splitted := parseMessage(message)
	op, _ := ToActionOperation(splitted[0])
	key, values := splitted[1], [][]byte{}
	for _, value := range splitted[2:] {
		values = append(values, []byte(value))
	}

	act := action{op: op, key: key, values: values, cli: cli}
	return act
}

// FIXME: cannot use strings package
func parseMessage(message string) []string {
	message = strings.TrimRight(message, "\r\n")
	splitted := strings.Split(message, ":")
	return splitted
}
