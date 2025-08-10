package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(connection)
	}
}


func handleConnection (connection net.Conn){

	defer connection.Close()

	for {
		buffer := make([]byte,256)

		_,err := connection.Read(buffer)

		if err != nil && err == io.EOF {
			break;
		}

		header := readHeader(buffer)

		writeHeader(header,connection)
	}
	
}
