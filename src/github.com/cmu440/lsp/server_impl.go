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
	RequestReadChannel chan bool
	BlockReadChannel chan bool
	ReadChannel chan *Message
	ErrorReadChannel chan int
	idMap map[int](clientData)
	addressMap map[*lspnet.UDPAddr] (clientData)
	mainReadBuf []*Message
}

type clientData struct {
	connId int
	seqNo int
	addr *lspnet.UDPAddr
	SendData bool
	TimeOut bool
	epochCount int
	readStart int
	writeStart int
	readBuffer []*Message
	writeBuffer []*Message
	quitChannel chan bool
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
		RequestReadChannel: make(chan bool),
		BlockReadChannel: make(chan bool),
		ReadChannel: make(chan *Message),
		ErrorReadChannel: make(chan int),
		idMap: make(map[int](clientData)),
		addressMap: make(map[*lspnet.UDPAddr](clientData)),
		mainReadBuf: make([]*Message, 0),
	}
	var err error
	go func() {
		err = myServer.StartListen(strconv.Itoa(port))
	}()
	return &myServer, err
}

func (s *server) Read() (int, []byte, error) {
	s.RequestReadChannel <- true
	for {
		select {
		case msg:= <-s.ReadChannel:
			return msg.ConnID, msg.Payload, nil
		case connId := <-s.ErrorReadChannel:
			return connId, nil, errors.New("Error while Reading")
			// add case for block read send! put timer.
		case <-s.BlockReadChannel:
			time.Sleep(time.Millisecond*time.Duration(10))
			s.RequestReadChannel <- true
		}
	}
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
		case addr := <-s.AddrChannel:
			client, ok := s.addressMap[addr]
			if !ok{
				s.connectionId++
				client = clientData{s.connectionId, 0, addr, false, false, 0,
				1, 1, make([]*Message, s.winSize+1), make([]*Message, 2*s.winSize+1),
				make(chan bool)}
				s.idMap[s.connectionId] = client
				s.addressMap[addr] = client

			}
			msg := NewAck(s.connectionId, 0)
			buf, _ := json.Marshal(msg)
			s.serverConnection.WriteToUDP(buf, addr)
		// Handle ack, data, write, close, closeall, resend, requestRead.
		case <-s.resendChannel:
			// for every client check for
			// 1) data which is not sent yet. Send ack for that so that client starts sending data.
			// 2) send unack data.
			for key := range s.idMap{
				client, ok := s.idMap[key]
				if !ok{
					continue
				}
				client.epochCount++
				if client.SendData == false{
					msg := NewAck(client.connId, 0)
					buf, _ := json.Marshal(msg)
					s.serverConnection.WriteToUDP(buf, client.addr)
				}
				for i:=0; i<s.winSize; i++{
					msg := client.writeBuffer[i]
					if msg!=nil && msg.SeqNum!=-1{
						// message is not yet acknowledged.
						buf, _ := json.Marshal(msg)
						s.serverConnection.WriteToUDP(buf, client.addr)
					}
				}

				// close connections if client timeout
				if client.epochCount > s.epochLimit{
					client.TimeOut = true
					// ToDo : Check for close channels.
				}
				s.idMap[key] = client
				s.addressMap[client.addr] = client
			}
		case msg := <-s.dataChannel:
			client, ok := s.idMap[msg.ConnID]
			if ok{
				client.SendData = true
				client.epochCount = 0

				buf, _ := json.Marshal(NewAck(client.connId, msg.SeqNum))
				s.serverConnection.WriteToUDP(buf, client.addr)

				index := msg.SeqNum - client.readStart
				if index < 0{
					// message is already rad. ACK dropped.
					fmt.Println("Data message already acked.", msg.SeqNum)
				}else if index < s.winSize{
					// save message or drop message.
					client.readBuffer[index] = msg
					var i int
					for i:=0; i<s.winSize; i++{
						if client.readBuffer[i] == nil{
							break
						}else{
							s.mainReadBuf = append(s.mainReadBuf, client.readBuffer[i])
						}
					}
					client.readStart += i
					client.readBuffer = client.readBuffer[i:]
					for len(client.readBuffer) <= s.winSize+1{
						client.readBuffer = append(client.readBuffer, nil)
					}
				}
				s.idMap[msg.ConnID] = client
				s.addressMap[client.addr] = client
			}else{
				fmt.Println("Target Client doesn't Exist", msg.ConnID)
			}
		case wd := <-s.writeChannel:
			client, ok := s.idMap[wd.connId]
			if ok{
				client.seqNo++
				msg := NewData(wd.connId, client.seqNo, wd.payload)
				// save data to its position on writebuf.
				index := client.seqNo - client.writeStart
				// Resize the write buffer accordingly.
				if index + 1 > len(client.writeBuffer){
					client.writeBuffer = extend(client.writeBuffer, index+1)
				}
				client.writeBuffer[index] = msg
				s.idMap[wd.connId] = client
				s.addressMap[client.addr] = client
				// send the message within the window immediately.
				if index < s.winSize{
					buf, _ := json.Marshal(msg)
					s.serverConnection.WriteToUDP(buf, client.addr)
				}
 			}else{
				fmt.Println("Client target does not exists.")
			}
		case msg := <-s.ackChannel:
			// receive acks from client.
			client, ok := s.idMap[msg.ConnID]
			if ok{
				client.epochCount = 0
				if msg.SeqNum != 0{
					// update and slide write window and send data in new window
					index := msg.SeqNum - client.writeStart
					if index>=0{
						client.writeBuffer[index].SeqNum = -1
						i := slideTo(client.writeBuffer, s.winSize)
						client.writeStart += i
						// slide the window.
						client.writeBuffer = client.writeBuffer[i:]
						// make sure the size is sufficient.
						if len(client.writeBuffer) < s.winSize{
							client.writeBuffer = extend(client.writeBuffer, s.winSize)
						}
						// send message in new window.
						for j:=0; j<s.winSize; j++{
							newMsg := client.writeBuffer[j]
							if newMsg!=nil && newMsg.SeqNum != -1{
								buf, _ := json.Marshal(newMsg)
								s.serverConnection.WriteToUDP(buf, client.addr)
							}
						}
					}
				}
				s.idMap[msg.ConnID] = client
				s.addressMap[client.addr] = client
			}else{
				fmt.Println("Target Client does not exist")
			}
		// case with requesting to read.
		case <-s.RequestReadChannel:
			if len(s.mainReadBuf) > 0{
				msg:=s.mainReadBuf[0]
				s.ReadChannel <- msg
				s.mainReadBuf = s.mainReadBuf[1:]
			}else{
				
			}
		}

	}
}
