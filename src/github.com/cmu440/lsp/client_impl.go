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
	epochCount int // have not received any message from server.
	isConn bool
	isSendData bool
	isTimeout bool
	dataChannel chan  *Message
	ackChannel chan *Message
	readChannel chan *Message
	writeChannel chan clientWriteData
	connectionSuccessChannel chan Message
	setConnectionIdChannel chan int
	leaveChannel chan bool
	resendChannel chan bool
	requiredIdChannel chan bool
	getIdChannel chan int
	requiredReadChannel chan bool
	blockReadChannel chan bool
	errorReadChannel chan bool

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
		epochCount: 0,
		isConn: false,
		isSendData: false,
		isTimeout: false,
		connectionSuccessChannel: make(chan Message),
		setConnectionIdChannel: make(chan int),
		leaveChannel: make(chan bool),
		resendChannel: make(chan bool),
		requiredIdChannel: make(chan bool),
		getIdChannel: make(chan int),
		requiredReadChannel: make(chan bool),
		errorReadChannel: make(chan bool),
		blockReadChannel: make(chan bool),
	}

	err := myClient.StartDial(hostport)
	return &myClient, err
}

func (c *client) ConnID() int {
	go func(){
		c.requiredIdChannel <- true
	}()

	for{
		select {
		case <- c.leaveChannel:
			return 0
		case id:= <- c.getIdChannel:
			return id
		}
	}
	return 0
}

func (c *client) Read() ([]byte, error) {

	c.requiredReadChannel <- true

	for{
		select {
		case <-c.blockReadChannel:
			time.Sleep(time.Duration(100)*time.Millisecond)
			c.requiredReadChannel <- true
		case msg:= <-c.readChannel:
			return msg.Payload, nil
		case <- c.errorReadChannel:
			return nil, errors.New("Error during Read!")
		}
	}

	return nil, errors.New("No Option matched?")
}

func (c *client) Write(payload []byte) error {
	c.writeChannel <- clientWriteData{MsgData, 0, payload}
	return nil
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
	// keep ticking. If quit channel comes, stop the ticker.
	// Else resend if ticker runs out.
	for{
		select {
		case <-c.leaveChannel:
			fmt.Println("Quiting the Channel")
			c.ticker.Stop()
			return
		case <-c.ticker.C:
			c.resendChannel <-true
		}
	}
}

func (c *client) handleMessages(){
	for{
		select {
		case <- c.resendChannel:
			// Resend might be of 3 types.
			// 1. connection ack from server. Resend connection message
			// 2. if server is ready to send data. Send heartbeat message if now
			// 3. if there is any unack message. resend
			c.epochCount++
			if !c.isConn{
				// resend connection message
				buf, _ := json.Marshal(NewConnect())
				c.connection.Write(buf)
				// block and read ack from server
				readBuf := make([]byte, 2000)
				n, err := c.connection.Read(readBuf)
				msg := Message{}
				if err == nil{
					json.Unmarshal(readBuf[0:n], &msg)
					if msg.Type == MsgAck{
						c.connectionSuccessChannel <- msg
					}
				}
			}else{
				if !c.isSendData{
					// if the server has not send data. Resend Ack.
					msg := NewAck(c.connectionId, 0)
					buf, _ := json.Marshal(msg)
					c.connection.Write(buf)
				}
				for i:=0; i<c.winSize; i++{
					msg:= c.writeBuffer[i]
					if msg!=nil && msg.SeqNum!=-1{
						// Message is not yet acknowledged.
						buf, _ := json.Marshal(msg)
						c.connection.Write(buf)
					}
				}
			}
			if c.epochCount > c.epochLimit{
				//  if not responding.
				c.isTimeout = true
				close(c.leaveChannel)
				c.connection.Close()
			}
		case msg:= <- c.dataChannel:
			// mark isSendData as true.
			// set epochCount as 0 as you have just received a message from server,
			c.isSendData = true
			c.epochCount = 0
			// write ack to server.
			buf, _ := json.Marshal(NewAck(c.connectionId, msg.SeqNum))
			c.connection.Write(buf)
			index:= msg.SeqNum - c.readBufferStart
			if index < 0{
				fmt.Println("Data message already acked " , msg.SeqNum)

			}else if index < c.winSize{
				c.readBuffer[index] = msg
				var i int
				for i=0; i<c.winSize; i++{
					if c.readBuffer[i] == nil{
						// no message received.
						break
					}else{
						c.mainReadBuffer = append(c.mainReadBuffer, c.readBuffer[i])
					}
				}
				// side the window.
				c.readBufferStart+=1
				c.readBuffer = c.readBuffer[i:]
				// make sure the readbuf size if winSize+1.
				for len(c.readBuffer) < c.winSize+1{
					c.readBuffer = append(c.readBuffer, nil)
				}
			}
		}
	}
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