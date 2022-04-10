package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

type client struct {
	serverReader *bufio.Reader
	serverWriter *bufio.Writer
}

func NewClient(conn net.Conn) *client {
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	return &client{reader, writer}

}

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's echoed response to standard output. Whether or
// not you add any code to this file will not affect your grade.
func main() {
	conn, _ := net.Dial("tcp", fmt.Sprintf("%v:%d", defaultHost, defaultPort))
	cli := NewClient(conn)
	for {
		// Send a response back to person contacting us.
		log.Println("tick.")
		_, writeErr := cli.serverWriter.WriteString("tick.\n")
		if writeErr != nil {
			fmt.Println("Error writing:", writeErr.Error())
		}
		cli.serverWriter.Flush()

		response, _ := cli.serverReader.ReadString('\n')
		log.Println("Response: " + response)
		time.Sleep(1 * time.Second)
	}
}
