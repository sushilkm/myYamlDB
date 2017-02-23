package common

import (
	"math/rand"
	"strings"
	"time"
)

// DBPort to start database service on
const DBPort = 7999

// DBLocation location where databases would be created
const DBLocation = "./dbDIR"

const newLineEncodingString = "-n-e-w-l-i-n-e-"

func encodeNewLine(fileContent string) string {
	return strings.Replace(fileContent, "\n", newLineEncodingString, -1)
}

func decodeNewLine(fileContent string) string {
	return strings.Replace(string(fileContent), newLineEncodingString, "\n", -1)
}

const spaceEncodingString = "-s-p-a-c-e-"

func encodeSpace(fileContent string) string {
	return strings.Replace(fileContent, " ", spaceEncodingString, -1)
}

func decodeSpace(fileContent string) string {
	return strings.Replace(fileContent, spaceEncodingString, " ", -1)
}

// EncodeFileContent encodes file-content (new-line and spaces)
func EncodeFileContent(fileContent []byte) string {
	return encodeNewLine(encodeSpace(string(fileContent)))

}

// DecodeFileContent decodes file-content (new-line and spaces)
func DecodeFileContent(fileContent string) string {
	return decodeSpace(decodeNewLine(fileContent))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

//GenerateRowID regenerates row-id
func GenerateRowID(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)

}

func init() {
	rand.Seed(time.Now().UnixNano())
}
