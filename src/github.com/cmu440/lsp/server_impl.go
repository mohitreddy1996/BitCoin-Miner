// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"../lspnet"
	"time"
	"strconv"
	"fmt"
	"encoding/json"
)

type server struct {
	winSize int
	connectionId int
	serverConnection *lspnet.UDPConn
	ticker *time.Ticker
	epochLimit int
	quitAllChannel chan bool
	AddrChannel chan *lspnet.UDPAddr
	dataChannel chan *Message
	ackChannel chan *Message
	resendChannel chan bool
	closeChannel chan int
	writeChannel chan WriteData
}

type WriteData struct {
	connId int
	seqNum int
	payload []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	myServer := server{
		winSize: params.WindowSize,
		connectionId: 0,
		ticker: time.NewTicker(time.Millisecond*time.Duration(params.EpochMillis)),
		epochLimit: params.EpochLimit,
		quitAllChannel: make(chan bool),
		AddrChannel: make(chan *lspnet.UDPAddr),
		dataChannel: make(chan *Message),
		ackChannel: make(chan *Message),
		resendChannel: make(chan bool),
		closeChannel: make(chan int),
		writeChannel: make(chan WriteData),
	}
	var err error
	go func() {
		err = myServer.StartListen(strconv.Itoa(port))
	}()
	return &myServer, err
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connID int, payload []byte) error {
	s.writeChannel <- WriteData{connID, 0, payload}
	return nil
}

func (s *server) CloseConn(connID int) error {
	go func() {
		s.closeChannel <- connID
	}()
	return nil
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

func (s *server) StartListen(port string) error {
	UDPAddr, err := lspnet.ResolveUDPAddr("udp", ":"+port)
	if err!=nil{
		fmt.Println("Unable to resolve UDP Address ", port)
		return err
	}
	s.serverConnection, err = lspnet.ListenUDP("udp", UDPAddr)
	if err!=nil{
		fmt.Println("Unable to listen on Port ", port)
		return err
	}

	go s.ticking()
	go s.handleMessages()

	for{
		select {
		case <-s.quitAllChannel:
			fmt.Println("Server Quit Listener")
			return nil
		default:
			// check and read message from UDP connection and check its types.4
			// write handle messages to operate on respective channels.
			buf := make([]byte, 2000)
			n, addr, err:= s.serverConnection.ReadFromUDP(buf)
			if err!=nil{
				return err
			}
			msg := Message{}
			json.Unmarshal(buf[0:n], &msg)
			switch msg.Type {
			case MsgConnect:
				s.AddrChannel <- addr
			case MsgData:
				s.dataChannel <- &msg
			case MsgAck:
				s.ackChannel <- &msg
			}
		}
	}
	return err
}

func (s *server) ticking() {
	for{
		select {
		case <-s.quitAllChannel:
			fmt.Println("Server Quit Ticket")
			s.ticker.Stop()
			return
		case <-s.ticker.C:
			s.resendChannel <- true
		}
	}
}

func (s *server) handleMessages(){
	for{
		select {
		// handle all channel cases. All resend, receive message etc.
		}
	}
}
