package sdorm

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

// NewDB returns a new DB using the provided `conn`, a sql database
// connection.
func NewDB(conn *sql.DB) DB {
	return DB{inner: conn}
}

// Closes db's database connection.
func (db *DB) Close() error {
	return db.inner.Close()
}

/*
	TableName analyzes a struct, v, and returns a single string, equal
	to the name of that struct's type, converted to underscore_case.
	Refer to the specification of underscore_case, below.
	Example usage:
	type MyStruct struct {
	...
	}
	TableName(&MyStruct{}) ==> "my_struct"
*/
func TableName(result interface{}) string {
	table_name := reflect.TypeOf(result).String()
	table_name = strings.Split(table_name, ".")[1]
	return camelToSnake(table_name)
}

/*
	Types to allow clients to specify filters in Find, Update, and Delete
	Each column name, operator code, and column value triplet makes up one filter condition.
	For Filter, the keys are column names and the values are of type FilterArg.
	For FieldArg, the keys are operator codes and the values are field values.

	Valid operator codes are: "lt" for less than, "gt" for greater than, "leq" for
	less than or equal to, "geq" for greater than or equal to, "eq" for equal to,
	"neq" for not equal to, "in" for in a set of values, and "nin" for not in a set of values.

	For all operators excluding "in" and "nin", the field value should only be a single value.
	For "in" and "nin", the field value should be an array of values.

	See the comment above addFilter for example usage.
*/
type FilterArg map[string]interface{}
type Filter map[string]FilterArg

/*
	Helper method for clients needing to construct a Filter type

	Example usage:
	filter := make(Filter)
	addFilter(filter, "Name", "eq", "Nick")
	addFilter(filter, "FullName", "in", []interface{}{"Nick", "Will"})
	findArgs.andFilter = filter
*/
func addFilter(filter Filter, field string, operator string, value interface{}) {
	if _, ok := filter[field]; !ok {
		// if there does not exist a filter for that field
		filter[field] = make(FilterArg)
	}
	filter[field][operator] = value
}

/*
	Type to allow clients to specify row sorting in Find

	Each inner string array should be of length 2, where the first string is the column name,
	and the second string must be "ASC" (sort by ascending order) or "DESC" (descending order).
	The order of the string arrays matter. The rows are first sorted by the first column, then the second, and so on.

	See the comment above addOrder for example usage.
*/
type OrderBy [][]string

/*
	Helper method for clients needing to construct an OrderBy type
	Example usage:
	orderBy := new(OrderBy)
	addOrder(orderBy, "ClassYear", "ASC")
	addOrder(orderBy, "Age", "DESC")
	findArgs.orderBy = orderBy
*/
func addOrder(orderBy *OrderBy, field string, order string) {
	fieldOrder := []string{field, order}
	*orderBy = append(*orderBy, fieldOrder)
}

/*
	Type for second argument to Delete or Update
	- andFilter: a Filter data type (see definition of Filter for more info)
*/
type DeleteOrUpdateArgs struct {
	andFilter Filter
}

/*
	Type for third argument to Update

	The keys are column names and the values are the new value for that column.

	See the comment above addUpdate for example usage.
*/
type Updates map[string]interface{}

/*
	Helper method for clients needing to construct an Updates type (third
	argument to Update)
	Example usage:
	updates := make(Updates)
	addUpdate(updates, "FullName", "Katie")
	addUpdate(updates, "Age", 15)
*/
func addUpdate(updates Updates, field string, value interface{}) {
	updates[field] = value
}

/*
	Type for second argument to Find
	- projection: an array of field names (likely strings)
	- andFilter: a Filter data type (see definition of Filter for more info)
	- orderBy: an OrderBy data type (see definition of OrderBy for more info)
	- limit: a positive int capping the number of returned rows
*/
type FindArgs struct {
	projection []interface{}
	andFilter  Filter
	orderBy    OrderBy
	limit      int
}

/*
	Find queries a database for all rows in a given table,
	and stores all matching rows in the slice provided as an argument.
	Various args in the form of FindArgs can be provided (see the comment
	above the FindArgs type definition for more details).

	The argument `result` will be a pointer to an empty slice of models.

	Find panics if the generated SQL query string is invalid, or if the
	table does not exist.

	Example usage to find UserComment entries in the database:
	type UserComment struct = { ... }
	result := []UserComment{}
	Set filter and orderBy as specified above in addFilter and addOrder methods above.
	filter := ...
	orderBy := ...
	args := FindArgs{
		projection: []interface{}{"Column1", "Column2", ...}
		andFilter: filter
		orderBy: orderBy
		limit: 5
	}
	db.Find(&result, args)
*/
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
			if !stringInSlice(val.Type().Field(i).Name, args.projection) {
				continue
			}
			ordered_projection[j] = val.Type().Field(i).Name
			j++
		}
	}
	if j != len(ordered_projection) {
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

	// add PROJECTED columns to query
	query := fmt.Sprintf("SELECT %v FROM %v", projected_columns, TableName(result))

	// convert each column name to camel case
	snake_projection := make([]interface{}, len(ordered_projection))
	for i := 0; i < len(ordered_projection); i++ {
		snake_projection[i] = camelToSnake(ordered_projection[i].(string))
	}

	// construct query with projected columns
	query = fmt.Sprintf(query, snake_projection...)

	// add WHERE filters if necessary
	query += buildWhereString(args.andFilter)

	// add ORDER BY
	if len(args.orderBy) > 0 {
		orderByFields := make([]string, 0)
		for _, orderField := range args.orderBy {
			orderByFields = append(orderByFields, camelToSnake(orderField[0])+" "+orderField[1])
		}
		query += " ORDER BY " + strings.Join(orderByFields, ", ")
	}

	// add row LIMIT
	// ignore LIMIT value if invalid
	if args.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", args.limit)
	}

	// execute query
	rows, _ := db.inner.Query(query)

	defer rows.Close()

	// invalid query results in nil rows
	if rows == nil {
		log.Panic("Invalid database query provided!")
	}

	// store column names
	cols := columnNames(res.Interface())
	// replace column names with projection if necessary
	if len(ordered_projection) > 0 {
		cols = snake_projection
	}

	// stores list of column types
	fields := make([]interface{}, len(cols))

	// fields array stores a pointer to the "type" of each column
	j = 0
	for i := 0; i < val.NumField(); i++ {
		// if we have a projection, but the current field is not in the project, skip
		if len(ordered_projection) > 0 && !stringInSlice(val.Type().Field(i).Name, ordered_projection) {
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
			if len(ordered_projection) > 0 && !stringInSlice(val.Type().Field(i).Name, ordered_projection) {
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

/*
	Create adds the specified model to the appropriate database table.
	The table for the model *must* already exist, and Create() panics
	if it does not.

	Optionally, at most one of the fields of the provided `model`
	might be annotated with the tag `dorm:"primary_key"`. If such a
	field exists, Create() should ignore the provided value of that
	field, overwriting it with the auto-incrementing row ID.
	This ID is given by the value of last_inserted_rowid(),
	returned from the underlying sql database.
*/
func (db *DB) Create(model interface{}) {
	tablename := db.checkTableExists(model)

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

	query := fmt.Sprintf("INSERT or REPLACE INTO %v(%v) VALUES(%v)", tablename, strings.Join(cols, ","), strings.Join(placeholder, ","))

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

/*
	Deletes rows in a given table from a database.
	Returns the number of rows deleted.

	The argument `model` is a struct that represents the table schema.
	The struct fields within `model` are unused.

	Args in the form of DeleteOrUpdateArgs can be provided (see the comment
	above the DeleteOrUpdateArgs type definition for more details).

	Delete panics if the generated SQL query string is invalid, or if the
	table does not exist.

	Example usage to delete some UserComment entries in the database:
	type UserComment struct = { ... }
	model := []UserComment{}
	Set filter specified above in the addFilter method above.
	filter := ...
	args := DeleteOrUpdateArgs{
		andFilter: filter
	}
	rows_deleted := db.Delete(&model, args)
*/
func (db *DB) Delete(model interface{}, args DeleteOrUpdateArgs) int {
	tablename := db.checkTableExists(model)
	query := fmt.Sprintf("DELETE FROM %v", tablename)

	// add WHERE filters if necessary
	query += buildWhereString(args.andFilter)

	delete_res, err := db.inner.Exec(query)
	if err != nil {
		log.Panic(err)
	}

	rows_affected, err := delete_res.RowsAffected()
	if err != nil {
		log.Panic(err)
	}

	return int(rows_affected)
}

/*
	Updates field values for rows in a given table from a database.
	Returns the number of rows update.

	The argument `model` is a struct that represents the table schema.
	The struct fields within `model` are unused.

	Args in the form of DeleteOrUpdateArgs can be provided (see the comment
	above the DeleteOrUpdateArgs type definition for more details).

	The argument `update` specifies the new values to be set for each column
	in the affected rows.

	Update panics if the generated SQL query string is invalid, if the
	table does not exist, or if a passed-in datatype in the Update parameter
	does not match its type in the SQL db.

	Example usage to update some UserComment entries in the database:
	type UserComment struct = { ... }
	model := []UserComment{}
	Set filter specified above in the addFilter method above.
	filter := ...
	args := DeleteOrUpdateArgs{
		andFilter: filter
	}
	rows_updated := db.Update(&model, args)
*/
func (db *DB) Update(model interface{}, args DeleteOrUpdateArgs, update Updates) int {
	tablename := db.checkTableExists(model)
	query := fmt.Sprintf("UPDATE %v", tablename)

	new_fields := make([]string, 0)
	for field := range update {
		// verify that types match those in model
		expected_type := reflect.ValueOf(model).Elem().FieldByName(field).Type()
		actual_type := reflect.TypeOf(update[field])
		if expected_type != actual_type {
			log.Panicf("Type of field %v in Update is %v but should be %v!", field, actual_type, expected_type)
		}

		// construct COL=NEW_VAL in query string
		new_field := fmt.Sprintf("%v=%v", camelToSnake(field), update[field])
		if reflect.TypeOf(update[field]) == reflect.TypeOf("") {
			new_field = fmt.Sprintf("%v='%v'", camelToSnake(field), update[field])
		}
		new_fields = append(new_fields, new_field)
	}

	// SET COL1=NEW_VAL1, COL2=NEW_VAL2...
	query += " SET " + strings.Join(new_fields, ",")

	// add WHERE filters if necessary
	query += buildWhereString(args.andFilter)

	update_res, err := db.inner.Exec(query)
	if err != nil {
		log.Panic(err)
	}

	rows_affected, err := update_res.RowsAffected()
	if err != nil {
		log.Panic(err)
	}

	return int(rows_affected)
}

/* ------------------------------------------------------------ */
/* HELPER METHODS                                               */
/* ------------------------------------------------------------ */

// Given a Filter, build the WHERE portion of a SQL query
// Returns empty string if no filter specified
func buildWhereString(andFilter Filter) string {
	whereString := ""
	if len(andFilter) > 0 {
		// an array of "field_name operator value"
		filters := make([]string, 0)
		for field_name := range andFilter {
			fields_filters := andFilter[field_name]
			for field_operator := range fields_filters {
				operator := ""

				// map operator code to SQL operator string
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
				case "in":
					operator = "IN"
				case "nin":
					operator = "NOT IN"
				default:
					log.Panic("Invalid filter operator provided!")
				}

				// build COL OPERATOR VALUE string
				arg := fields_filters[field_operator]
				condition_str := fmt.Sprintf("%v%v%v", camelToSnake(field_name), operator, arg)

				// check type is string
				if reflect.TypeOf(arg) == reflect.TypeOf("") {
					condition_str = fmt.Sprintf("%v%v'%v'", camelToSnake(field_name), operator, arg)
				}

				if operator == "IN" || operator == "NOT IN" {
					values := make([]string, 0)
					for _, value := range fields_filters[field_operator].([]interface{}) {
						new_value := value
						// check type is string
						if reflect.TypeOf(value) == reflect.TypeOf("") {
							new_value = fmt.Sprintf("'%v'", new_value)
						}
						values = append(values, fmt.Sprintf("%v", new_value))
					}
					list_str := fmt.Sprintf("(%v)", strings.Join(values, ","))
					// COL IN (a, b, ...)
					condition_str = fmt.Sprintf("%v %v %v", camelToSnake(field_name), operator, list_str)
				}

				filters = append(filters, condition_str)
			}
		}

		// construct SQL WHERE string with conditions AND'd together
		whereString = " WHERE " + strings.Join(filters, " AND ")
	}
	return whereString
}

// Given a model, check if its corresponding table exists in db
func (db *DB) checkTableExists(model interface{}) string {
	tablename := TableName(model)
	query := fmt.Sprintf("SELECT * FROM %v", tablename)
	rows, err := db.inner.Query(query)

	if err != nil {
		log.Panic(fmt.Sprintf("Table %v not found!", tablename))
	}
	for rows.Next() {
		// must do this to prevent table not found error
	}

	defer rows.Close()

	return tablename
}

// Converts camel case to underscore (snake) case
// Source: https://stackoverflow.com/a/56616250
func camelToSnake(camel string) string {
	matchFirstCap := regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap := regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(camel, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}

// Checks if string a is in slice list
// Source: https://stackoverflow.com/questions/10485743/contains-method-for-a-slice
func stringInSlice(a string, list []interface{}) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

/*
	Analyzes a struct, v, and returns a list of strings,
	one for each of the public fields of v.
	The i'th string returned should be equal to the name of the i'th
	public field of v, converted to underscore_case.

	Example usage:
	type MyStruct struct {
		ID int64
		UserName string
	}
	columnNames(&MyStruct{}) ==> []string{"id", "user_name"}
*/
func columnNames(v interface{}) []interface{} {
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
