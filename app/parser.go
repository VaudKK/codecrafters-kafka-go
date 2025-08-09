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

type Numeric interface{
	int8 | int16 | int32 | int64
}

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
	buf := new(bytes.Buffer)

	//write the correlation ID
	writeBuffer(buf, header.CorrelationID)

	var response []byte

	// write the error code
	if header.RequestAPIVersion < 0 || header.RequestAPIVersion > 4 {
		writeBuffer(buf, UNSUPPORTED_VERSION)
	}else{
		writeBuffer(buf,NO_ERROR)
	}

	//// write api version

	// one byte for api version array length + 1
	writeBuffer(buf,int8(2))

	// api key
	writeBuffer(buf,header.RequestAPIKey)

	// api min version
	writeBuffer(buf,int16(0))

	// api max version
	writeBuffer(buf,int16(4))

	// api tag
	writeBuffer(buf,int8(0))

	messageSize := getMessageSize(buf)
	response = append(messageSize,buf.Bytes()...)


	connection.Write(response)
}

func getMessageSize(buf *bytes.Buffer) []byte {
	sizeBuffer := new(bytes.Buffer)
	length := buf.Len()

	writeBuffer(sizeBuffer, int32(length))
	return sizeBuffer.Bytes()
}


func writeBuffer[T Numeric](buf *bytes.Buffer,data T){
	err := binary.Write(buf,binary.BigEndian,data)
	if err != nil {
		fmt.Println(err)
		return
	}
}

