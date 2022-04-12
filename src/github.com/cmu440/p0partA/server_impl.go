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

const bufferSize = 500

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
	countChannel        chan countRequest
	actionChannel       chan actionRequest
}

type countRequest struct {
	op      CountOperation
	channel chan int
}

type actionRequest struct {
	op     ActionOperation
	key    string
	values [][]byte
	cli    *client
}

type response struct {
	key   string
	value string
}

type client struct {
	id     string
	reader *bufio.Reader
	writer *bufio.Writer
	conn   net.Conn
	// FIXME: think of a better name
	channel chan response
}

func (cli client) String() string {
	return fmt.Sprintf("Client: %v", cli.id)
}

func (cli client) isBufferFull() bool {
	return len(cli.channel) >= bufferSize
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	// TODO: implement this!
	clientPool := map[string]*client{}
	registrationChannel := make(chan *client)
	removalChannel := make(chan string)
	actionChannel := make(chan actionRequest)
	countChannel := make(chan countRequest)
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
	// Create channel to get response. Supports multiple routines asking for counts.
	ch := make(chan int)
	kvs.countChannel <- countRequest{op: Active, channel: ch}
	activeCount := <-ch
	return activeCount
}

func (kvs *keyValueServer) CountDropped() int {
	// Create channel to get response. Supports multiple routines asking for counts.
	ch := make(chan int)
	kvs.countChannel <- countRequest{op: Dropped, channel: ch}
	droppedCount := <-ch
	return droppedCount
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
		case request := <-kvs.countChannel:
			switch request.op {
			case Active:
				request.channel <- len(kvs.clientPool)
			case Dropped:
				request.channel <- kvs.dropped
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
		cli := registerClient(conn, kvs)
		go handleRead(cli, kvs)
		go handleWrite(cli, kvs)
	}
}

func handleKvActions(kvs *keyValueServer) {
	for act := range kvs.actionChannel {
		// TODO: handle failures
		switch act.op {
		case Get:
			values := kvs.store.Get(act.key)
			for _, value := range values {
				if !act.cli.isBufferFull() {
					resp := response{act.key, string(value)}
					act.cli.channel <- resp
				}
			}
		case Put:
			kvs.store.Put(act.key, act.values[0])
		case Update:
			kvs.store.Update(act.key, act.values[0], act.values[1])
		case Delete:
			kvs.store.Delete(act.key)
		}
	}
}

func handleRead(cli *client, kvs *keyValueServer) {
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

		// Check if client closed or terminated connection
		case io.EOF:
			removeClient(cli.id, kvs)
			// fmt.Printf("[%v] disconnected\n", cli.id)
			return
		default:
			fmt.Printf("[%v] error\n", cli.id)
		}
	}
}

func handleWrite(cli *client, kvs *keyValueServer) {
	for resp := range cli.channel {
		message := fmt.Sprintf("%v:%v\n", resp.key, resp.value)
		_, writeErr := cli.writer.WriteString(message)
		if writeErr != nil {
			fmt.Println("Error writing:", writeErr.Error())
		}
		cli.writer.Flush()
	}
}

func registerClient(conn net.Conn, kvs *keyValueServer) *client {
	id := conn.RemoteAddr().String()
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	channel := make(chan response, bufferSize)
	cli := &client{id: id, reader: reader, writer: writer, conn: conn, channel: channel}
	// FIXME: only sends to registration channel now, does not wait for confirmation
	kvs.registrationChannel <- cli
	return cli
}

func removeClient(id string, kvs *keyValueServer) {
	kvs.removalChannel <- id
}

func messageToAction(message string, cli *client) actionRequest {
	// Parse the message into actions
	splitted := parseMessage(message)
	op, _ := ToActionOperation(splitted[0])
	key, values := splitted[1], [][]byte{}
	for _, value := range splitted[2:] {
		values = append(values, []byte(value))
	}

	act := actionRequest{op: op, key: key, values: values, cli: cli}
	return act
}

// FIXME: cannot use strings package
func parseMessage(message string) []string {
	message = strings.TrimRight(message, "\r\n")
	splitted := strings.Split(message, ":")
	return splitted
}
