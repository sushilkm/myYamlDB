package engine

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/sushilkm/myYamlDB/common"
	"github.com/sushilkm/myYamlDB/models"
	"gopkg.in/yaml.v2"
)

// Table name is always to be prefixed with db name,
// so format of table-name is db-name:table-name
// if the format does not matches reject table-name
// also reject table-name if it has ":"

func (db *DBEngine) parseTableName() ([]string, error) {
	tablePieces := strings.Split(db.cmdArgs[0], ":")
	if len(tablePieces) > 2 {
		return nil, errors.New("INVALID TABLE-NAME, VALID TABLE-NAME SHOULD NOT HAVE ':'. PLEASE REENTER TABLE NAME AS <DB-NAME>:<TABLE-NAME>")
	}
	return tablePieces, nil
}

func (db *DBEngine) initializeTable() error {
	tablePieces, err := db.parseTableName()
	if err != nil {
		return err
	}
	if len(tablePieces) < 2 {
		return errors.New("INVALID TABLE-NAME")
	}
	tableFileName := strings.ToUpper(filepath.Join(common.DBLocation, tablePieces[0]+dbFileSuffix, tablePieces[1])) + tableFileSuffix
	if _, err = os.Stat(tableFileName); os.IsNotExist(err) {
		return ioutil.WriteFile(tableFileName, []byte("{}\n"), 0644)
	}
	return errors.New("TABLE '" + strings.ToUpper(db.cmdArgs[0]) + "' ALREADY EXISTS")
}

func (db *DBEngine) listTables() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("NO DATABASE IS USED")
	}

	dbLocation := filepath.Join(common.DBLocation, db.cmdArgs[0]) + dbFileSuffix
	if _, err := os.Stat(dbLocation); os.IsNotExist(err) {
		return "", errors.New("INVALID DB-NAME, DATABASE DOES NOT EXISTS")
	}

	tables, err := ioutil.ReadDir(dbLocation)
	if err != nil {
		fmt.Printf("Error while listing tables: (%v)\n", err)
		return "", errors.New(dbEngineError)
	}
	if len(tables) == 0 {
		return "NO TABLES EXIST", nil
	}

	var tableList string
	for _, table := range tables {
		if !strings.HasSuffix(table.Name(), tableFileSuffix) {
			continue
		}
		if tableList != "" {
			tableList += "\n"
		}
		tableList += strings.Trim(table.Name(), tableFileSuffix)
	}
	return strings.ToUpper(tableList), nil
}

func (db *DBEngine) createTable() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("INVALID TABLE-NAME, CANNOT CREATE TABLE")
	}

	if err := db.initializeTable(); err != nil {
		return "", err
	}

	return fmt.Sprintf(`TABLE '%s' created.`, strings.ToUpper(db.cmdArgs[0])), nil
}

func (db *DBEngine) deleteTable() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("INVALID TABLE-NAME, CANNOT DELETE TABLE")
	}

	tablePieces, err := db.parseTableName()
	if err != nil {
		return "", err
	}
	tableFileName := filepath.Join(common.DBLocation, tablePieces[0]+dbFileSuffix, tablePieces[1]) + tableFileSuffix
	if _, err = os.Stat(tableFileName); os.IsNotExist(err) {
		return "", errors.New("TABLE DOES NOT EXISTS")
	}

	if err := os.Remove(tableFileName); err != nil {
		return "", err
	}

	return fmt.Sprintf(`TABLE '%s:%s' deleted.`, tablePieces[0], tablePieces[1]), nil
}

func (db *DBEngine) readTable() (string, error) {
	if len(db.cmdArgs) != 1 {
		return "", errors.New("INVALID TABLE-NAME, CANNOT READ TABLE")
	}

	tablePieces, err := db.parseTableName()
	if err != nil {
		return "", err
	}
	tableFileName := filepath.Join(common.DBLocation, tablePieces[0]+dbFileSuffix, tablePieces[1]) + tableFileSuffix
	if _, err = os.Stat(tableFileName); os.IsNotExist(err) {
		return "", errors.New("TABLE DOES NOT EXISTS")
	}
	tableData, _ := ioutil.ReadFile(tableFileName)
	if tbl, valid := models.ParseYaml(tableData); valid {
		return tbl.ToString(), nil
	}

	return "", errors.New("INVALID TABLE DATA")
}

func (db *DBEngine) writeTable() (string, error) {
	if len(db.cmdArgs) < 1 {
		return "", errors.New("INVALID TABLE-NAME, CANNOT WRITE TABLE")
	}
	if len(db.cmdArgs) < 2 {
		return "", errors.New("INVALID TABLE-DATA, CANNOT WRITE TABLE")
	}

	tablePieces, err := db.parseTableName()
	if err != nil {
		return "", err
	}
	tableFileName := filepath.Join(common.DBLocation, tablePieces[0]+dbFileSuffix, tablePieces[1]) + tableFileSuffix
	if _, err = os.Stat(tableFileName); os.IsNotExist(err) {
		return "", errors.New("TABLE DOES NOT EXISTS")
	}

	if db.cmdArgs[1] == "NO-DATA" {
		return "", errors.New("NO TABLE-DATA PROVIDED")
	}
	if db.cmdArgs[1] == "INVALID-DATA" {
		return "", errors.New("INVALID TABLE-DATA PROVIDED")
	}
	newData := common.DecodeFileContent(db.cmdArgs[1])
	// Now compare the columns of new-data to column list of old data
	// if they do not match then reject the request

	newRecord, valid := models.ParseYamlRecord([]byte(newData))
	if !valid {
		return "", errors.New("INVALID TABLE-DATA PROVIDED")
	}

	tableData, _ := ioutil.ReadFile(tableFileName)
	existingTable, valid := models.ParseYaml(tableData)
	if !valid {
		return "", errors.New("INVALID TABLE DATA IN EXISTING TABLE")
	}

	var newColumnList, oldColumnList []string

	for key := range newRecord.Columns {
		newColumnList = append(newColumnList, key)
	}

	for _, record := range existingTable.Records {
		for columnName := range record.Columns {
			oldColumnList = append(oldColumnList, columnName)
		}
		break
	}

	//Now compare old and new column lists

	var columnListMatches = true
	for _, newValue := range newColumnList {
		var columnFound bool
		for _, oldValue := range oldColumnList {
			if newValue == oldValue {
				columnFound = true
				break
			}
		}
		if !columnFound {
			columnListMatches = false
			break
		}
	}
	if !columnListMatches {
		fmt.Printf("OLD-COLUMN (%v)\n", oldColumnList)
		fmt.Printf("NEW-COLUMN (%v)\n", newColumnList)
		return "", errors.New("INVALID TABLE-DATA, COLUMNS DON'T MATCH WITH EXISTING TABLE")
	}

	myMap := make(map[string]interface{})
	var dataMap map[string]interface{}
	if err := yaml.Unmarshal([]byte(newData), &dataMap); err != nil {
		return "", errors.New("INVALID TABLE-DATA")
	}
	myMap["row_id_"+common.GenerateRowID(len(newData))] = dataMap

	recordToBeWritten, err := yaml.Marshal(myMap)
	if err != nil {
		return "", errors.New("INVALID TABLE-DATA")
	}

	f, err := os.OpenFile(tableFileName, os.O_APPEND|os.O_WRONLY, 0600)
	defer f.Close()
	if err != nil {
		fmt.Printf("Error while writing tables: (%v)\n", err)
		return "", errors.New(dbEngineError)
	}

	if _, err := f.WriteString(string(recordToBeWritten)); err != nil {
		fmt.Printf("Error while writing tables: (%v)\n", err)
		return "", errors.New(dbEngineError)
	}

	return fmt.Sprintf(`TABLE '%s:%s' WRITTEN.`, tablePieces[0], tablePieces[1]), nil
}
