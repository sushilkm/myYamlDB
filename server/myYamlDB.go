package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	// only needed below for sample processing
	"github.com/sushilkm/myYamlDB/common"
	"github.com/sushilkm/myYamlDB/engine"
)

func portAvailable(port int) bool {
	conn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

var dbObject = engine.DBEngine{}

func main() {

	var port = strconv.Itoa(common.DBPort)
	commandLineArgs := os.Args
	if len(commandLineArgs) > 2 {
		fmt.Println("USAGE: myYamlDB <port>")
		return
	}
	if len(commandLineArgs) >= 2 {
		port = commandLineArgs[1]
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		fmt.Println("DB port should be a number")
		os.Exit(1)
	}
	if !portAvailable(intPort) {
		fmt.Println("DB port is already in USE")
		os.Exit(1)
	}

	fmt.Println("Launching server...")

	// listen on all interfaces
	ln, _ := net.Listen("tcp", ":"+port)

	// accept connection on port
	conn, _ := ln.Accept()

	// run loop forever (or until ctrl-c)
	for {
		// will listen for message to process ending in newline (\n)
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil && err.Error() == "EOF" {
			conn.Close()
			conn, _ = ln.Accept()
			continue
		}
		fmt.Print("Received command:", string(message))

		// sample process for string received
		newmessage := strings.ToUpper(message)

		err = dbObject.MakeCommand(string(message))
		if err != nil {
			fmt.Println(err.Error())
			newmessage = err.Error()
		} else if output, err := dbObject.ExecuteCommand(); err != nil {
			fmt.Println(err.Error())
			newmessage = err.Error()
		} else {
			newmessage = output
		}
		// send new string back to client
		dataLength := strconv.Itoa(len(newmessage))
		conn.Write([]byte(dataLength + "\n" + newmessage))
	}
}
