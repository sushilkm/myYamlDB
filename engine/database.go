package engine

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/sushilkm/myYamlDB/common"
)

func (db *DBEngine) initializeDatabase() error {
	_, err := os.Stat(common.DBLocation)
	if os.IsNotExist(err) {
		return os.Mkdir(common.DBLocation, 0700)
	}
	return nil
}

func (db *DBEngine) checkDatabase() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("INVALID DB-NAME")
	}

	dbPath := filepath.Join(common.DBLocation, strings.ToUpper(db.cmdArgs[0])) + dbFileSuffix
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return "", errors.New("DB DOES NOT EXISTS")
	}

	return strings.ToUpper(db.cmdArgs[0]), nil
}
func (db *DBEngine) listDatabases() (string, error) {
	if _, err := os.Stat(common.DBLocation); os.IsNotExist(err) {
		return "NO DATABASES EXIST", nil
	}

	databases, err := ioutil.ReadDir(common.DBLocation)
	if err != nil {
		fmt.Printf("Error while listing databases: (%v)\n", err)
		return "", errors.New(dbEngineError)

	}
	if len(databases) == 0 {
		return "NO DATABASES EXIST", nil
	}
	var dbList string
	for _, database := range databases {
		if !strings.HasSuffix(database.Name(), dbFileSuffix) {
			continue
		}
		if dbList != "" {
			dbList += "\n"
		}
		dbList += strings.TrimSuffix(database.Name(), dbFileSuffix)
	}

	return strings.ToUpper(dbList), nil
}

func (db *DBEngine) createDatabase() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("INVALID DB-NAME, CANNOT CREATE DATABASE")
	}

	if err := db.initializeDatabase(); err != nil {
		return "", err
	}

	dbPath := filepath.Join(common.DBLocation, strings.ToUpper(db.cmdArgs[0])) + dbFileSuffix
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		return "", errors.New("DB ALREADY EXISTS")
	}

	if err := os.Mkdir(dbPath, 0700); err != nil {
		return "", err
	}

	return fmt.Sprintf(`DB '%s' created.`, db.cmdArgs[0]), nil
}

func (db *DBEngine) deleteDatabase() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("INVALID DB-NAME, CANNOT DELETE DATABASE")
	}

	dbPath := filepath.Join(common.DBLocation, strings.ToUpper(db.cmdArgs[0])) + dbFileSuffix
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return "", errors.New("DB DOES NOT EXISTS")
	}
	tables, err := ioutil.ReadDir(dbPath)
	if err != nil {
		fmt.Printf("Error while deleting database: (%v)\n", err)
		return "", errors.New(dbEngineError)
	}
	if len(tables) > 0 {
		return "", errors.New("CANNOT DELETE DATABASE, IT HAS TABLES")
	}
	if err := os.Remove(dbPath); err != nil {
		return "", err
	}

	return fmt.Sprintf(`DB '%s' deleted.`, db.cmdArgs[0]), nil
}
