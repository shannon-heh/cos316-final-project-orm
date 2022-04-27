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
func ColumnNames(v interface{}) []interface{} {
	val := reflect.ValueOf(v).Elem()
	cols := []interface{}{}
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


type FilterArg map[string]interface{}
type Filter map[string]FilterArg

func addFilter(filter Filter, field string, operator string, value interface{}) {
	if _, ok := filter[field]; !ok {
		// if there does not exist a filter for that field
		filter[field] = make(FilterArg)
	}
	filter[field][operator] = value
}

type OrderBy [][]string

func addOrder(orderBy *OrderBy, field string, order string) {
	fieldOrder := []string{field, order}
	*orderBy = append(*orderBy, fieldOrder)
}

// arguments for Find
type FindArgs struct {
	projection []interface{}
	andFilter  Filter
	orderBy    OrderBy
	limit	int
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
	// get struct type (e.g. dorm.User)
	elem := reflect.TypeOf(result).Elem().Elem()

	// create a new struct of the same type
	res := reflect.New(elem)
	val := res.Elem()
	j := 0

	// fix order of args.projection to match order of fields in struct
	ordered_projection := make([]interface{}, len(args.projection))
	if len(args.projection) > 0 {
		for i := 0; i < val.NumField(); i++ {
			if (!stringInSlice(val.Type().Field(i).Name, args.projection)) {
				continue
			}
			ordered_projection[j] = val.Type().Field(i).Name
			j++
		}
	}
	if (j != len(ordered_projection)) {
		log.Panic("Invalid projection column provided!")
	}

	// insert placeholders for projected columns
	projected_columns := "*"
	if len(ordered_projection) > 0 {
		projected_placeholders := make([]string, len(ordered_projection))
		for i := range ordered_projection {
			projected_placeholders[i] = "%v"
		}
		projected_columns = strings.Join(projected_placeholders, ", ")
	}

	tablename := TableName(result)
	query := fmt.Sprintf("SELECT %v FROM %v", projected_columns, tablename)

	// add AND filters
	if len(args.andFilter) > 0 {
		// an array of "field_name operator value"
		filters := make([]string, 0)
		for field_name := range args.andFilter {
			fields_filters := args.andFilter[field_name]
			for field_operator := range fields_filters {
				operator := ""
				switch field_operator {
				case "lt":
					operator = "<"
				case "gt":
					operator = ">"
				case "eq":
					operator = "="
				case "neq":
					operator = "!="
				case "leq":
					operator = "<="
				case "geq":
					operator = ">="
				default:
					log.Panic("Invalid filter operator provided!")
				}
				arg := fields_filters[field_operator]
				condition_str := fmt.Sprintf("%v%v%v", camelToSnake(field_name), operator, arg)
				switch arg.(type) {
				case string:
					condition_str = fmt.Sprintf("%v%v'%v'", camelToSnake(field_name), operator, arg)
				}
				filters = append(filters, condition_str)
			}
		}
		query += " WHERE " + strings.Join(filters, " AND ")
	}

	// add ORDER BY
	if len(args.orderBy) > 0 {
		orderByFields := make([]string, 0)
		for _, orderField := range args.orderBy {
			orderByFields = append(orderByFields, camelToSnake(orderField[0]) + " " + orderField[1])
		}
		query += " ORDER BY " + strings.Join(orderByFields, ", ")
	}

	// add row LIMIT
	if args.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d",args.limit);
	}

	// convert each column name to camel case
	snake_projection := make([]interface{}, len(ordered_projection))
	for i:= 0; i < len(ordered_projection); i++ {
		snake_projection[i] = camelToSnake(ordered_projection[i].(string))
	}

	// construct query with projected columns
	query = fmt.Sprintf(query, snake_projection...)
	
	// execute query
	rows, _ := db.inner.Query(query)

	defer rows.Close()

	// invalid query results in nil rows
	if (rows == nil) {
		log.Panic("Invalid database query provided!")
	}

	// store column names
	cols := ColumnNames(res.Interface())
	// replace column names with projection if necessary
	if (len(ordered_projection) > 0) {
		cols = snake_projection
	}

	// stores list of column types
	fields := make([]interface{}, len(cols)) // array of interfaces

	// fields array stores a pointer to the "type" of each column
	j = 0
	for i := 0; i < val.NumField(); i++ {
		// if we have a projection, but the current field is not in the project, skip
		if (len(ordered_projection) > 0 && !stringInSlice(val.Type().Field(i).Name, ordered_projection)) {
			continue
		}
		field := reflect.New(val.Field(i).Type()).Interface()
		fields[j] = field
		j++
	}

	// modify original result
	arr := reflect.ValueOf(result).Elem()
	for rows.Next() {
		new_struct := reflect.New(elem).Elem()
		// stores each row's values into the fields array (temporarily)
		rows.Scan(fields...)
		j := 0
		for i := 0; i < val.NumField(); i++ {
			// if we have a projection, but the current field is not in the project, skip
			if (len(ordered_projection) > 0 && !stringInSlice(val.Type().Field(i).Name, ordered_projection)) {
				continue			
			}
			// sets each field value in the struct
			new_struct.Field(i).Set(reflect.ValueOf(fields[j]).Elem())
			j++
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

// converts camel case to underscore (snake) case
// source: https://stackoverflow.com/a/56616250
func camelToSnake(camel string) string {
	matchFirstCap := regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap := regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(camel, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}

// checks if string a is in slice list
// source: https://stackoverflow.com/questions/10485743/contains-method-for-a-slice
func stringInSlice(a string, list []interface{}) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}