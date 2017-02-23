package engine

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	dbEngineError   = "DB ENGINE ERROR"
	dbFileSuffix    = ".db"
	tableFileSuffix = ".tbl"
)

var (
	validCommands = [...]string{
		"CREATE-DB",
		"LIST-DBS",
		"DELETE-DB",
		"USE-DB",
		"CREATE TABLE",
		"LIST-TABLES",
		"DELETE-TABLE",
		"READ-TABLE",
		"WRITE-TABLE",
		"FILTER",
		"SORT",
	}
	cmdArguments = map[string]string{
		"CREATE-DB":    "1",
		"LIST-DBS":     "0",
		"DELETE-DB":    "1",
		"USE-DB":       "1",
		"CREATE-TABLE": "1",
		"DELETE-TABLE": "1",
		"LIST-TABLES":  "1",
		"READ-TABLE":   "1",
		"WRITE-TABLE":  "2",
		"FILTER":       "multi",
		"SORT":         "multi",
	}
)

// DBEngine database engine
type DBEngine struct {
	cmd     string
	cmdArgs []string
}

//MakeCommand forms db Command
func (db *DBEngine) MakeCommand(message string) error {
	if validated, err := db.validateMessage(message); !validated {
		return err
	}

	cmd := strings.Fields(message)

	db.cmd = cmd[0]
	if len(cmd) > 1 {
		db.cmdArgs = cmd[1:]
	}
	return nil
}

// ExecuteCommand executes given command
func (db *DBEngine) ExecuteCommand() (string, error) {
	if db.cmd == "" {
	}
	switch strings.ToUpper(db.cmd) {
	case "CREATE-DB":
		return db.createDatabase()
	case "LIST-DBS":
		return db.listDatabases()
	case "DELETE-DB":
		return db.deleteDatabase()
	case "USE-DB":
		return db.checkDatabase()
	case "CREATE-TABLE":
		return db.createTable()
	case "LIST-TABLES":
		return db.listTables()
	case "DELETE-TABLE":
		return db.deleteTable()
	case "READ-TABLE":
		return db.readTable()
	case "WRITE-TABLE":
		return db.writeTable()

	default:
		return "", errors.New("INVALID COMMAND")

	}
}

func (db *DBEngine) validateMessage(message string) (bool, error) {
	dbCmd := strings.Fields(message)
	// Match command if it is a valid command
	cmdArgsCount, ok := cmdArguments[strings.ToUpper(dbCmd[0])]
	if !ok {
		return false, errors.New("INVALID COMMAND: " + dbCmd[0])
	}

	// Match if the number of arguments is valid
	if cmdArgsCount != "multi" {
		argsCount, err := strconv.Atoi(cmdArgsCount)
		if err != nil {
			fmt.Printf("Error while validating message: (%v)\n", err)
			return false, errors.New(dbEngineError)
		}
		if argsCount != len(dbCmd)-1 {
			return false, errors.New("INVALID NUMBER OF ARGUMENTS")
		}
	}
	return true, nil
}
