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

type KafkaRequestHeader struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

type KafkaResponse struct {
	CorrelationID int32
	ErrorCode int16
	APIVersions []APIVersion
	ThrottleTime int32
	TagBuffer int8
}

type APIVersion struct {
	APIKey int16
	MinVersion int16
	MaxVersion int16
	TagBuffer int8
}

func readHeader(request []byte) KafkaRequestHeader {
	size := request[:4]
	apiKey := request[4:6]
	version := request[6:8]
	correlationId := request[8:12]

	return KafkaRequestHeader{
		MessageSize: int32(binary.BigEndian.Uint32(size)),
		RequestAPIKey: int16(binary.BigEndian.Uint16(apiKey)),
		RequestAPIVersion: int16(binary.BigEndian.Uint16(version)),
		CorrelationID: int32(binary.BigEndian.Uint32(correlationId)),
	}
}

func writeHeader(header KafkaRequestHeader, connection net.Conn){

	response := KafkaResponse{
		CorrelationID: header.CorrelationID,
		ThrottleTime: int32(0),
		TagBuffer: int8(0),
	}

	if header.RequestAPIVersion < 0 || header.RequestAPIVersion > 4 {
		response.ErrorCode =  UNSUPPORTED_VERSION
	}else{
		response.ErrorCode = NO_ERROR
	}

	apiVersions := make([]APIVersion, 0)

	apiVersion := APIVersion{
		APIKey: header.RequestAPIKey,
		MinVersion: int16(0),
		MaxVersion: int16(4),
		TagBuffer: int8(0),
	}

	apiDescribeToPartition := APIVersion{
		APIKey: int16(75),
		MinVersion: int16(0),
		MaxVersion: int16(0),
		TagBuffer: int8(0),
	}

	apiVersions = append(apiVersions, apiVersion)
	apiVersions = append(apiVersions, apiDescribeToPartition)

	response.APIVersions = apiVersions
	
	resp := convertResponseHeaderToBytes(response)

	connection.Write(resp)
}

func getMessageSize(buf *bytes.Buffer) []byte {
	sizeBuffer := new(bytes.Buffer)
	length := buf.Len()

	writeBuffer(sizeBuffer, int32(length))
	return sizeBuffer.Bytes()
}


func convertResponseHeaderToBytes(kafkaResponse KafkaResponse) []byte{
	buf := new(bytes.Buffer)
	writeBuffer(buf, kafkaResponse.CorrelationID)

	var response []byte
	writeBuffer(buf, kafkaResponse.ErrorCode)

	// one byte for api version array length + 1
	writeBuffer(buf,int8(len(kafkaResponse.APIVersions)+1))

	// write api versions
	for _, apiVersion := range kafkaResponse.APIVersions {
		writeBuffer(buf,apiVersion.APIKey)
		writeBuffer(buf,apiVersion.MinVersion)
		writeBuffer(buf,apiVersion.MaxVersion)
		writeBuffer(buf,apiVersion.TagBuffer)
	}

	// throttle time
	writeBuffer(buf,kafkaResponse.ThrottleTime)

	// tag buffer
	writeBuffer(buf, kafkaResponse.TagBuffer)

	messageSize := getMessageSize(buf)
	response = append(messageSize,buf.Bytes()...)

	return response
}


func writeBuffer[T Numeric](buf *bytes.Buffer,data T){
	err := binary.Write(buf,binary.BigEndian,data)
	if err != nil {
		fmt.Println(err)
		return
	}
}

