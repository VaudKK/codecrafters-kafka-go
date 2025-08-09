package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

const (
	NO_ERROR = int16(0)
	UNSUPPORTED_VERSION = int16(35)
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

func writeHeader(header Header, connection net.Conn){
	messageSize := []byte{0,0,0,0}
	buf := new(bytes.Buffer)

	//write the correlation ID
	err := binary.Write(buf,binary.BigEndian,header.CorrelationID)

	if err != nil {
		fmt.Println(err)
		return
	}

	var response []byte

	// write the error code
	if header.RequestAPIVersion < 0 || header.RequestAPIVersion > 4 {
		err = binary.Write(buf,binary.BigEndian,UNSUPPORTED_VERSION)
		if err != nil {
			fmt.Println(err)
			return
		}
	}else{
		err = binary.Write(buf,binary.BigEndian,NO_ERROR)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	// write api version
	
	err = binary.Write(buf,binary.BigEndian,int32(18))
	if err != nil {
		fmt.Println(err)
		return
	}
	
	response = append(messageSize,buf.Bytes()...)


	connection.Write(response)
}