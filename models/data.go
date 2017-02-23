package models

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

// DataColumn database record
type DataColumn struct {
	ColumnData interface{}
}

// DataRecord database record
type DataRecord struct {
	Columns map[string]DataColumn
}

// DataTable database table
type DataTable struct {
	Records map[string]DataRecord
}

// ToString returns string representation of table
func (tbl *DataTable) ToString() string {
	var tableData [][]string
	var numberOfColumns int

	//Find number of columns
	for _, value := range tbl.Records {
		numberOfColumns = len(value.Columns)
		break
	}

	//Populate column-names list
	tableColumns := make([]string, numberOfColumns)
	for _, value := range tbl.Records {
		var i int
		for key := range value.Columns {
			tableColumns[i] = key
			i++
		}
		break
	}

	//Read column data as per column-name sequence
	for _, tableRecord := range tbl.Records {
		tmpRecord := make([]string, numberOfColumns)
		for key, value := range tableRecord.Columns {
			columnIndex := findColumnIndex(tableColumns, key)
			if columnIndex == -1 {
				return ""
			}

			switch reflect.TypeOf(value.ColumnData).String() {
			case "string":
				tmpRecord[columnIndex] = value.ColumnData.(string)
			case "int":
				tmpRecord[columnIndex] = strconv.Itoa(value.ColumnData.(int))
			case "float64":
				tmpRecord[columnIndex] = strconv.FormatFloat(value.ColumnData.(float64), 'f', -1, 64)
			case "bool":
				tmpRecord[columnIndex] = strconv.FormatBool(value.ColumnData.(bool))
			default:
				tmpRecord[columnIndex] = reflect.TypeOf(value.ColumnData).String()
			}
			if strings.Contains(reflect.TypeOf(value.ColumnData).String(), "interface") {
				tmpRecord[columnIndex] = readInterfaceArray(value.ColumnData)
			}
		}
		tableData = append(tableData, tmpRecord)
	}

	var returnTableString = strings.Join(tableColumns, "|")

	for _, value := range tableData {
		returnTableString += "\n" + strings.Join(value, "|")
	}
	return returnTableString
}

// ParseYaml parses yaml document content
func ParseYaml(content []byte) (*DataTable, bool) {
	var verificationMap map[string]interface{}
	if err := yaml.Unmarshal(content, &verificationMap); err != nil {
		fmt.Printf("1.1 >> Error while parsing data: (%v)\n", err)
		return nil, false
	}

	var table DataTable
	table.Records = make(map[string]DataRecord)
	for key, value := range verificationMap {
		var tmpRecord DataRecord
		var yamlRecord map[string]interface{}
		dataValue, err := yaml.Marshal(value)
		if err != nil {
			fmt.Printf("2.1 >> Error while byting data: (%v)\n", err)
			return nil, false
		}
		if err := yaml.Unmarshal(dataValue, &yamlRecord); err != nil {
			fmt.Printf("2.2 >> Error while parsing data: (%v)\n", err)
			return nil, false
		}

		tmpRecord.Columns = make(map[string]DataColumn)
		for columnName, columnValue := range yamlRecord {
			tmpRecord.Columns[columnName] = DataColumn{ColumnData: columnValue}
		}
		table.Records[key] = tmpRecord
	}
	return &table, true
}

func findColumnIndex(columns []string, column string) int {
	for columnIndex, columnName := range columns {
		if columnName == column {
			return columnIndex
		}
	}
	return -1
}
func readInterfaceArray(data interface{}) string {
	return "arrays-not-supported-currently"
}

// ParseYamlRecord parses yaml document content
func ParseYamlRecord(content []byte) (*DataRecord, bool) {
	var verificationMap map[string]interface{}
	var dataRecord DataRecord
	if err := yaml.Unmarshal(content, &verificationMap); err != nil {
		fmt.Printf("1.1 >> Error while parsing data: (%v)\n", err)
		return nil, false
	}
	dataRecord.Columns = make(map[string]DataColumn)
	for columnName, columnValue := range verificationMap {
		dataRecord.Columns[columnName] = DataColumn{ColumnData: columnValue}
	}
	return &dataRecord, true
}
