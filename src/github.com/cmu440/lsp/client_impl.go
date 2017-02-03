// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"time"
	"../lspnet"
	"encoding/json"
)

type client struct {
	// TODO: implement this!
	winSize int
	connectionId int
	seqNum int
	serverAddr *lspnet.UDPAddr
	connection *lspnet.UDPConn
	ticker *time.Ticker
	epochLimit int
	dataChannel chan  *Message
	ackChannel chan *Message
	readChannel chan *Message
	writeChannel chan clientWriteData
	connectionSuccessChannel chan Message
	setConnectionIdChannel chan int
	leaveChannel chan bool

	//buffers
	readBuffer []*Message
	writeBuffer []*Message
	mainReadBuffer []*Message
	readBufferStart int // start sequence Number of read buffer
	writeBufferStart int // start sequence number of write buffer.
}

type clientWriteData struct{
	messageType MsgType //{conn, data, ack}
	seqNum int
	payload []byte //data.
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	myClient := client{
		winSize: params.WindowSize,
		seqNum: 0,
		ticker: time.NewTicker(time.Millisecond*time.Duration(params.EpochMillis)),
		ackChannel: make(chan *Message),
		readChannel: make(chan *Message),
		writeChannel: make(chan clientWriteData),
		readBuffer: make([]*Message, params.WindowSize+1),
		writeBuffer: make([]*Message, params.WindowSize+1),
		mainReadBuffer: make([]*Message, 0),
		readBufferStart: 0,
		writeBufferStart: 0,
		connectionSuccessChannel: make(chan Message),
		setConnectionIdChannel: make(chan int),
		leaveChannel: make(chan bool),
	}

	err := myClient.StartDial(hostport)
	return &myClient, err
}

func (c *client) ConnID() int {
	return -1
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}

func (c *client) StartDial(hostport string) error{
	var err error
	// resolve address.
	c.serverAddr, err = lspnet.ResolveUDPAddr("udp", hostport)
	if err!=nil{
		fmt.Println("Unable to resolve server address ", hostport)
		return err
	}

	// start ticking.
	go c.ticking()
	// write a function to handle messages sent/received at channels.
	go c.handleMessages()

	// dial udp.
	c.connection, err = lspnet.DialUDP("udp", nil, c.serverAddr)

	if err!=nil{
		fmt.Println("Cannot dial to port ", hostport)
		return err
	}

	fmt.Println("dial to ", hostport)

	// send (connect, 0, 0) message
	c.writeChannel <- clientWriteData{MsgConnect, 0, nil}
	// handle this case in handleMessages.

	// wait for ack.
	// 2000 bytes buffer. (as per given in the pdf.)
	buffer := make([]byte, 2000)
	message := Message{}
	// read into the buffer.
	data, err := c.connection.Read(buffer)
	if err!=nil{
		fmt.Println("Unable to read from connection.")
		message = <-c.connectionSuccessChannel
	}else{
		// use marshal package to parse as json.
		json.Unmarshal(buffer[0:data], &message)
	}

	switch message.Type {
	case MsgAck:
		c.setConnectionIdChannel <-message.ConnID
	default:
		return errors.New("Unable to connect!")
	}

	// start listen to data or ack continuously.
	go c.listen()

	return nil
}

func (c *client) ticking(){

}

func (c *client) handleMessages(){

}

func (c *client) listen() error {
	// listen to data or ack received.
	for{
		select {
		case <-c.leaveChannel:
			fmt.Println("quit!!")
			return nil
		default:
			buffer := make([]byte, 2000)
			n, err := c.connection.Read(buffer)
			if err != nil{
				fmt.Println("Client cannot read message")
				return err
			}
			// unMarshal and put it in data channel or ack channel.
			message := Message{}
			json.Unmarshal(buffer[0: n], &message)
			switch message.Type {
			case MsgData:
				c.dataChannel <- &message
			case MsgAck:
				c.ackChannel <- &message
			}
		}

	}
	return nil
}