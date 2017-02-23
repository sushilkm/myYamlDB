package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/sushilkm/myYamlDB/common"
	"github.com/sushilkm/myYamlDB/models"
)

func checkIfTableCommand(cmd string) bool {
	switch strings.ToUpper(strings.Fields(cmd)[0]) {
	case "CREATE-TABLE":
		return true
	case "LIST-TABLES":
		return true
	case "DELETE-TABLE":
		return true
	case "READ-TABLE":
		return true
	case "WRITE-TABLE":
		return true
	default:
		return false
	}
}

func main() {
	var host = "127.0.0.1"
	var port = strconv.Itoa(common.DBPort)
	commandLineArgs := os.Args
	if len(commandLineArgs) > 3 {
		fmt.Println("USAGE: myYamlDBClient <host> <port>")
		return
	}
	if len(commandLineArgs) >= 2 {
		host = commandLineArgs[1]
	}
	if len(commandLineArgs) == 3 {
		port = commandLineArgs[2]
	}

	// connect to this socket
	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		fmt.Printf("Failed to get connection: (%v)\n", err)
		os.Exit(1)
	}
	for {
		// read in input from stdin
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Text to send: ")
		text, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Cannot read: (%v)\n", err)
			continue
		}
		if text == "\n" {
			fmt.Println("No input provided")
			continue
		}

		if checkIfTableCommand(text) {
			cmdPieces := strings.Fields(text)
			dbName := strings.Trim(os.Getenv("DB_NAME"), "\n")
			if dbName == "" {
				fmt.Println("OPEN A DATABASE FIRST: 'use-db <db-name>'")
				continue
			}
			if len(cmdPieces) > 1 {
				text = strings.Replace(text, cmdPieces[1], dbName+":"+cmdPieces[1], -1)
			} else {
				text = strings.Trim(text, "\n") + " " + dbName + "\n"
			}
		}

		// send to socket
		fmt.Fprintf(conn, writeTableCommand(text))
		// listen for reply
		message := readOutput(conn)
		if strings.HasPrefix(strings.ToUpper(text), "USE-DB") {
			fmt.Println("DEFAULT DB SET TO: " + message)
			os.Setenv("DB_NAME", message)
			continue
		}
		fmt.Println(message)
	}
}

func readOutput(reader io.Reader) string {
	var text string
	var bytesRead int
	bufferReader := bufio.NewReader(reader)

	// Find length of message received
	text, err := bufferReader.ReadString('\n')
	if err != nil {
		return text
	}
	messageLength, err := strconv.Atoi(strings.Trim(text, "\n"))
	if err != nil {
		return ""
	}
	text = ""

	// Now read rest of the message
	for bytesRead < messageLength {
		bytesRead++
		dataRead, err := bufferReader.ReadByte()
		if err != nil {
			break
		}
		text += string(dataRead)
	}
	return text
}

func writeTableCommand(commmandText string) string {

	cmdPieces := strings.Fields(commmandText)
	if strings.ToUpper(cmdPieces[0]) != "WRITE-TABLE" {
		return commmandText + "\n"
	}
	if len(cmdPieces) != 3 {
		return commmandText + "\n"
	}

	tableName := cmdPieces[1]
	docName := cmdPieces[2]

	// Read the document and verify yaml
	fileContent, err := ioutil.ReadFile(docName)
	if err != nil {
		return cmdPieces[0] + " " + cmdPieces[1] + " NO-DATA\n"
	}

	if _, valid := models.ParseYamlRecord(fileContent); !valid {
		return cmdPieces[0] + " " + cmdPieces[1] + " INVALID-DATA\n"
	}

	fmt.Printf("Writing data from file: %s to table: %s\n", docName, tableName)
	return cmdPieces[0] + " " + cmdPieces[1] + " " + common.EncodeFileContent(fileContent) + "\n"
}
