package main

import "fmt"
import "net"
import "bufio"
import "strings" // only needed below for sample processing

func main() {

	fmt.Println("Launching server...")

	// listen on all interface
	ln, _ := net.Listen("tcp", ":8081")

	// accept connection on port
	conn, _ := ln.Accept()

	// fun loop forever ( or util ctrl-c )
	for {
		// will listen for message to process ending in newline
		message, _ := bufio.NewReader(conn).ReadString('\n')

		// output message received
		fmt.Print("Message Received:", string(message))

		// sample process for string received
		newmessage := strings.ToUpper(message)
		// send new string back to client

		conn.Write([]byte(newmessage + "\n"))
	}

}
