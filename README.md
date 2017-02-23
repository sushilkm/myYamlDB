# myYamlDB

This projects aims at reading and storing data in the yaml format

In its current state project runs as server and client, it currently supports only one client connection



## Following list of functions have been currently implemented:

#### Run DB Server:
To run on default port 7999, just run:
```
myYamlDB
```
To run on a different port
```
myYamlDB <port>
```

#### connect to DB Server:
Just execute the client, it is defaulted to connect db-server running locally
To connect to DB-server runnning elsewhere:
```
myYamlDBClient <host> <port>
```

#### create database
```
create-db <db-name>
```
#### list databases
```
list-dbs
```
#### delete database
```
delete-db <db-name>
```
#### open database
```
use-db <db-name>
```

To execute table-level commands some database should be opened first, using `use-db`
#### create table
```
create-table <tbl-name>
```
#### list tables
```
list-tables
```
#### delete table
```
delete-table <table-name>
```
#### write data
```
write-table <table-name> <document-file-location>
```
#### read data
```
read-table <table-name>
```
