package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type Header struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

func readHeader(request []byte) Header {
	size := request[:4]
	apiKey := request[4:6]
	version := request[6:8]
	correlationId := request[8:12]

	return Header{
		MessageSize: int32(binary.BigEndian.Uint32(size)),
		RequestAPIKey: int16(binary.BigEndian.Uint16(apiKey)),
		RequestAPIVersion: int16(binary.BigEndian.Uint16(version)),
		CorrelationID: int32(binary.BigEndian.Uint32(correlationId)),
	}
}

func writeCorrelationId (correlationId int32, connection net.Conn){
	messageSize := []byte{0,0,0,0}
	buf := new(bytes.Buffer)

	err := binary.Write(buf,binary.BigEndian,correlationId)

	if err != nil {
		fmt.Println(err)
		return
	}

	response := append(messageSize,buf.Bytes()...)

	connection.Write(response)
}