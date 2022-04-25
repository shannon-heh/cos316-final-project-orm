package dorm

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"unicode"
)

// DB handle
type DB struct {
	inner *sql.DB
}

// NewDB returns a new DB using the provided `conn`,
// an sql database connection.
// This function is provided for you. You DO NOT need to modify it.
func NewDB(conn *sql.DB) DB {
	return DB{inner: conn}
}

// Close closes db's database connection.
// This function is provided for you. You DO NOT need to modify it.
func (db *DB) Close() error {
	return db.inner.Close()
}

// converts camel case to underscore (snake) case
// source: https://stackoverflow.com/a/56616250
func camelToSnake(camel string) string {
	matchFirstCap := regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap := regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(camel, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}

// ColumnNames analyzes a struct, v, and returns a list of strings,
// one for each of the public fields of v.
// The i'th string returned should be equal to the name of the i'th
// public field of v, converted to underscore_case.
// Refer to the specification of underscore_case, below.

// Example usage:
// type MyStruct struct {
//    ID int64
//    UserName string
// }
// ColumnNames(&MyStruct{})    ==>   []string{"id", "user_name"}
func ColumnNames(v interface{}) []string {
	val := reflect.ValueOf(v).Elem()
	cols := []string{}
	for i := 0; i < val.NumField(); i++ {
		colname := val.Type().Field(i).Name
		if unicode.IsLower([]rune(colname)[0]) {
			continue
		}
		colname_fixed := camelToSnake(colname)
		cols = append(cols, colname_fixed)
	}
	return cols
}

// TableName analyzes a struct, v, and returns a single string, equal
// to the name of that struct's type, converted to underscore_case.
// Refer to the specification of underscore_case, below.

// Example usage:
// type MyStruct struct {
//    ...
// }
// TableName(&MyStruct{})    ==>  "my_struct"
func TableName(result interface{}) string {
	table_name := reflect.TypeOf(result).String()
	table_name = strings.Split(table_name, ".")[1]
	return camelToSnake(table_name)
}

// arguments for Find
type FindArgs struct {
	projection []string
}

func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

// Find queries a database for all rows in a given table,
// and stores all matching rows in the slice provided as an argument.

// The argument `result` will be a pointer to an empty slice of models.
// To be explicit, it will have type: *[]MyStruct,
// where MyStruct is any arbitrary struct subject to the restrictions
// discussed later in this document.
// You may assume the slice referenced by `result` is empty.

// Example usage to find all UserComment entries in the database:
//    type UserComment struct = { ... }
//    result := []UserComment{}
//    db.Find(&result)

// NOTE: result is an array of structs (of the same type)
func (db *DB) Find(result interface{}, args FindArgs) {
	// args.projection is [] if client does not define it

	// construct query
	tablename := TableName(result)
	query := fmt.Sprintf("SELECT * FROM %v", tablename)

	// execute query
	rows, _ := db.inner.Query(query)

	defer rows.Close()

	// get struct type (e.g. dorm.User)
	elem := reflect.TypeOf(result).Elem().Elem()
	// create a new struct of the same type
	res := reflect.New(elem)

	// stores column names
	cols := ColumnNames(res.Interface())
	// stores list of column types
	fields := make([]interface{}, len(cols)) // array of interfaces

	// fields array stores a pointer to the "type" of each column
	val := res.Elem()
	for i := 0; i < val.NumField(); i++ {
		field := reflect.New(val.Field(i).Type()).Interface()
		fields[i] = field
	}

	// convert each projection column to snake case
	snake_projection := make([]string, len(args.projection))
	for i:= 0; i < len(args.projection); i++ {
		snake_projection[i] = camelToSnake(args.projection[i])
	}

	// modify original result 
	arr := reflect.ValueOf(result).Elem()
	for rows.Next() {
		new_struct := reflect.New(elem).Elem()
		// stores each row's values into the fields array (temporarily)
		rows.Scan(fields...)
		for i := 0; i < len(fields); i++ {
			// do not set field based on projection
			if (len(snake_projection) != 0 && !stringInSlice(cols[i], snake_projection)) {
				continue;
			}
			// sets each field value in the struct
			new_struct.Field(i).Set(reflect.ValueOf(fields[i]).Elem())
		}
		// append new struct to array
		arr.Set(reflect.Append(arr, new_struct))
	}
}

// First queries a database for the first row in a table,
// and stores the matching row in the struct provided as an argument.
// If no such entry exists, First returns false; else it returns true.

// The argument `result` will be a pointer to a model.
// To be explicit, it will have type: *MyStruct,
// where MyStruct is any arbitrary struct subject to the restrictions
// discussed later in this document.

// Example usage to find the first UserComment entry in the database:
//    type UserComment struct = { ... }
//    result := &UserComment{}
//    ok := db.First(result)
// with the argument), otherwise return true.
func (db *DB) First(result interface{}) bool {
	tablename := TableName(result)
	query := fmt.Sprintf("SELECT * FROM %v", tablename)
	rows, _ := db.inner.Query(query)

	defer rows.Close()

	elem := reflect.TypeOf(result).Elem() // struct
	res := reflect.New(elem)

	cols := ColumnNames(res.Interface())
	fields := make([]interface{}, len(cols))

	val := res.Elem()
	for i := 0; i < val.NumField(); i++ {
		field := reflect.New(val.Field(i).Type()).Interface()
		fields[i] = field
	}

	if !rows.Next() {
		return false
	}
	rows.Scan(fields...)

	the_struct := reflect.ValueOf(result).Elem()
	new_struct := reflect.New(elem).Elem()
	for i := 0; i < len(fields); i++ {
		new_struct.Field(i).Set(reflect.ValueOf(fields[i]).Elem())
	}
	the_struct.Set(new_struct)

	return true
}

// Create adds the specified model to the appropriate database table.
// The table for the model *must* already exist, and Create() should
// panic if it does not.

// Optionally, at most one of the fields of the provided `model`
// might be annotated with the tag `dorm:"primary_key"`. If such a
// field exists, Create() should ignore the provided value of that
// field, overwriting it with the auto-incrementing row ID.
// This ID is given by the value of last_inserted_rowid(),
// returned from the underlying sql database.
func (db *DB) Create(model interface{}) {
	tablename := TableName(model)
	query := fmt.Sprintf("SELECT * FROM %v", tablename)
	rows, err := db.inner.Query(query)
	for rows.Next() {
		// must do this to prevent table not found error
	}

	if err != nil {
		log.Panic(fmt.Sprintf("Table %v not found!", tablename))
	}

	defer rows.Close()

	elem := reflect.TypeOf(model).Elem()
	res := reflect.New(elem)

	cols := []string{}
	placeholder := []string{}
	fields := []interface{}{}

	v := reflect.ValueOf(res.Interface()).Elem()
	v_model := reflect.ValueOf(model).Elem()
	for i := 0; i < v.NumField(); i++ {
		colname := v.Type().Field(i).Name
		tag := v.Type().Field(i).Tag
		if tag == `dorm:"primary_key"` {
			// ignore PK column
			continue
		}
		if unicode.IsLower([]rune(colname)[0]) {
			continue
		}
		colname_fixed := camelToSnake(colname)
		cols = append(cols, colname_fixed)

		placeholder = append(placeholder, "?")
		fields = append(fields, v_model.Field(i).Interface())
	}

	query = fmt.Sprintf("INSERT or REPLACE INTO %v(%v) VALUES(%v)", tablename, strings.Join(cols, ","), strings.Join(placeholder, ","))

	insert_res, err := db.inner.Exec(query, fields...)
	if err != nil {
		log.Panic(err)
	}

	the_struct := reflect.ValueOf(model).Elem() // gets values in model struct
	new_struct := reflect.New(elem).Elem()      // creates new struct with same type as model
	v_model = reflect.ValueOf(model).Elem()
	for i := 0; i < v.NumField(); i++ {
		tag := v.Type().Field(i).Tag
		if tag == `dorm:"primary_key"` {
			// if PK tag, then update PK column with last insert ID
			id, _ := insert_res.LastInsertId()
			new_struct.Field(i).Set(reflect.ValueOf(&id).Elem()) // set id in struct
		} else {
			// otherwise, set field to itself
			new_struct.Field(i).Set(v_model.Field(i))
		}
	}
	the_struct.Set(new_struct)

}
