package sdorm

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func connectSQL() *sql.DB {
	conn, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	if err != nil {
		panic(err)
	}
	return conn
}

func createUserTable(conn *sql.DB) {
	_, err := conn.Exec(`create table user (
		full_name text,
		age int,
		class_year text,
		is_enrolled int
	)`)

	if err != nil {
		panic(err)
	}
}

// User Table Schema
type User struct {
	FullName   string
	Age        int
	ClassYear  string
	IsEnrolled bool
}

/*
	Helper method to test that field values in resulting row values match expected row values,
	and that the rows appear in the same order.
*/
func helperTestEquality(t *testing.T, results []User, expected []User) {
	if len(results) != len(expected) {
		t.Errorf("Expected %v rows but instead found %v rows", len(expected), len(results))
	}
	for i, result := range results {
		if result.FullName != expected[i].FullName {
			t.Errorf("Expected %v but instead found %v", expected[i].FullName, result.FullName)
		}
		if result.Age != expected[i].Age {
			t.Errorf("Expected %v but instead found %v", expected[i].Age, result.Age)
		}
		if result.ClassYear != expected[i].ClassYear {
			t.Errorf("Expected %v but instead found %v", expected[i].ClassYear, result.ClassYear)
		}
	}
}

/*
	Helper method to test that the number of rows updated or deleted matches its expected value.
*/
func helperTestIntEquality(t *testing.T, result int, expected int) {
	if result != expected {
		t.Errorf("Expected %v but instead got %v rows changed", expected, result)
	}
}

/*
	Helper method to verify that a block of code panics.
*/
func helperTestPanic(t *testing.T, theFunc func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic but none generated")
		}
	}()

	theFunc()
}

func TestProjection(t *testing.T) {
	fmt.Println(">>> PROJECTION TESTS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Senior", Age: 20}
	db.Create(&user_nick)
	db.Create(&user_shannon)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Only FullName")
	results := []User{}
	args := FindArgs{
		projection: []interface{}{"FullName"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Nick"},
		{FullName: "Shannon"},
	})

	fmt.Println("Test: Only Age and ClassYear")
	results = []User{}
	args = FindArgs{
		projection: []interface{}{"Age", "ClassYear"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{ClassYear: "Freshman", Age: 10},
		{ClassYear: "Senior", Age: 20},
	})

	fmt.Println("Test: Only ClassYear and Age")
	results = []User{}
	args = FindArgs{
		projection: []interface{}{"ClassYear", "Age"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{ClassYear: "Freshman", Age: 10},
		{ClassYear: "Senior", Age: 20},
	})

	fmt.Println("Test: Only FullName and Age")
	results = []User{}
	args = FindArgs{
		projection: []interface{}{"FullName", "Age"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Nick", Age: 10},
		{FullName: "Shannon", Age: 20},
	})

	fmt.Println("Test: All Results - Empty Projection Array")
	results = []User{}
	args = FindArgs{
		projection: []interface{}{},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
	})

	fmt.Println("Test: All Results - No Projection Array Specified")
	results = []User{}
	args = FindArgs{}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
	})

	helperTestPanic(t, func() {
		fmt.Println("Test: Non-existent Field")
		results = []User{}
		args = FindArgs{
			projection: []interface{}{"FullName", "FakeField"},
		}
		db.Find(&results, args)
	})
}

func TestFilter(t *testing.T) {
	fmt.Println(">>> FILTER TESTS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10, IsEnrolled: true}
	user_shannon := User{FullName: "Shannon", ClassYear: "Senior", Age: 20, IsEnrolled: false}

	db.Create(&user_nick)
	db.Create(&user_shannon)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Get FullName = Nick, Only Nick")
	results := []User{}
	filter := make(Filter)
	addFilter(filter, "FullName", "eq", "Nick")
	args := FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
	})

	fmt.Println("Test: Get Age < 15, Only Nick")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "Age", "lt", 15)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
	})

	fmt.Println("Test: Get Age >= 10, Both")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "Age", "geq", 10)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
	})

	fmt.Println("Test: Get Age < 0, None")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "Age", "lt", 0)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{})

	fmt.Println("Test: Get ClassYear = Senior and FullName = Shannon, Only Shannon")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "ClassYear", "eq", "Senior")
	addFilter(filter, "FullName", "eq", "Shannon")
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{user_shannon})

	fmt.Println("Test: Get Age >= 21 and FullName = Shannon, None")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "FullName", "eq", "Shannon")
	addFilter(filter, "Age", "geq", 21)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{})

	fmt.Println("Test: Get IsEnrolled = true, Only Shannon")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsEnrolled", "neq", true)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
	})

	fmt.Println("Test: Get IsEnrolled = false, Only Shannon")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsEnrolled", "eq", false)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
	})

	fmt.Println("Test: Get IsEnrolled = true and FullName = Nicj, Only Nick")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsEnrolled", "eq", true)
	addFilter(filter, "FullName", "gt", "Nicj")
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
	})

	fmt.Println("Test: Get FullName in ('Nick', 'Will')")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "FullName", "in", []interface{}{"Nick", "Will"})
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
	})

	fmt.Println("Test: Get FullName not in ('Nick', 'Will'), Age not in (10, 12)")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "FullName", "nin", []interface{}{"Nick", "Will"})
	addFilter(filter, "Age", "nin", []interface{}{10, 12})
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
	})

	helperTestPanic(t, func() {
		fmt.Println("Test: Get Name = Nick, None")
		results = []User{}
		filter = make(Filter)
		addFilter(filter, "Name", "eq", "Nick")
		args = FindArgs{
			andFilter: filter,
		}
		db.Find(&results, args)
	})

	helperTestPanic(t, func() {
		fmt.Println("Test: Get Name = Nick and Age = 10, None")
		results = []User{}
		filter = make(Filter)
		addFilter(filter, "Age", "eq", 10)
		addFilter(filter, "Name", "eq", "Nick")
		args = FindArgs{
			andFilter: filter,
		}
		db.Find(&results, args)
	})
}

func TestOrderBy(t *testing.T) {
	fmt.Println(">>> ORDER BY TESTS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Order by FullName ASC")
	results := []User{}
	orderBy := new(OrderBy)
	addOrder(orderBy, "FullName", "ASC")
	args := FindArgs{
		orderBy: *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
		user_will,
	})

	fmt.Println("Test: Order by FullName DESC")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "FullName", "DESC")
	args = FindArgs{
		orderBy: *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_will,
		user_shannon,
		user_nick,
	})

	fmt.Println("Test: Order by ClassYear ASC, Age DESC")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "ClassYear", "ASC")
	addOrder(orderBy, "Age", "DESC")
	args = FindArgs{
		orderBy: *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
		user_nick,
		user_will,
	})

	fmt.Println("Test: Order by Age DESC, ClassYear ASC")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "Age", "DESC")
	addOrder(orderBy, "ClassYear", "ASC")
	args = FindArgs{
		orderBy: *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
		user_will,
		user_nick,
	})

	helperTestPanic(t, func() {
		fmt.Println("Test: FakeField ASC, None")
		results = []User{}
		orderBy = new(OrderBy)
		addOrder(orderBy, "FakeField", "ASC")
		args = FindArgs{
			orderBy: *orderBy,
		}
		db.Find(&results, args)
	})

	helperTestPanic(t, func() {
		fmt.Println("Test: FullName DSC, None")
		results = []User{}
		orderBy = new(OrderBy)
		addOrder(orderBy, "FullName", "DSC")
		args = FindArgs{
			orderBy: *orderBy,
		}
		db.Find(&results, args)
	})
}

func TestLimit(t *testing.T) {
	fmt.Println(">>> LIMIT TESTS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: LIMIT 1")
	results := []User{}
	args := FindArgs{
		limit: 1,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
	})

	fmt.Println("Test: LIMIT 2")
	results = []User{}
	args = FindArgs{
		limit: 2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
	})

	fmt.Println("Test: LIMIT 4")
	results = []User{}
	args = FindArgs{
		limit: 4,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
		user_will,
	})

	fmt.Println("Test: LIMIT -1")
	results = []User{}
	args = FindArgs{
		limit: -1,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
		user_will,
	})
}

func TestFindFull(t *testing.T) {
	fmt.Println(">>> ALL TESTS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10, IsEnrolled: true}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20, IsEnrolled: false}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20, IsEnrolled: true}
	user_katie := User{FullName: "Katie", ClassYear: "Sophomore", Age: 30, IsEnrolled: false}
	user_albert := User{FullName: "Albert", ClassYear: "Senior", Age: 40, IsEnrolled: true}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)
	db.Create(&user_katie)
	db.Create(&user_albert)

	/* ------------------------------------------------------------ */

	// PROJECT + WHERE

	fmt.Println("Test: PROJECT FullName, IsEnrolled, WHERE ClassYear != Freshman, Age > 20")
	results := []User{}
	filter := make(Filter)
	addFilter(filter, "ClassYear", "neq", "Freshman")
	addFilter(filter, "Age", "gt", 20)
	args := FindArgs{
		projection: []interface{}{"FullName", "IsEnrolled"},
		andFilter:  filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Katie", IsEnrolled: false},
		{FullName: "Albert", IsEnrolled: true},
	})

	// add IN operator
	fmt.Println("Test: PROJECT Age, ClassYear, WHERE IsEnrolled = true")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsEnrolled", "eq", true)
	args = FindArgs{
		projection: []interface{}{"Age", "ClassYear"},
		andFilter:  filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{Age: 10, ClassYear: "Freshman"},
		{Age: 20, ClassYear: "Senior"},
		{Age: 40, ClassYear: "Senior"},
	})

	/* ------------------------------------------------------------ */

	// PROJECT + LIMIT

	fmt.Println("Test: PROJECT FullName, LIMIT 2")
	results = []User{}
	args = FindArgs{
		projection: []interface{}{"FullName"},
		limit:      2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Nick"},
		{FullName: "Shannon"},
	})

	/* ------------------------------------------------------------ */

	// PROJECT + ORDER BY

	fmt.Println("Test: PROJECT FullName, Age ORDER BY Age DESC, FullName ASC")
	results = []User{}
	orderBy := new(OrderBy)
	addOrder(orderBy, "Age", "DESC")
	addOrder(orderBy, "FullName", "ASC")
	args = FindArgs{
		projection: []interface{}{"FullName", "Age"},
		orderBy:    *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Albert", Age: 40},
		{FullName: "Katie", Age: 30},
		{FullName: "Shannon", Age: 20},
		{FullName: "Will", Age: 20},
		{FullName: "Nick", Age: 10},
	})

	/* ------------------------------------------------------------ */

	// WHERE + LIMIT

	fmt.Println("Test: WHERE Age != 20, LIMIT 2")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "Age", "neq", 20)
	args = FindArgs{
		andFilter: filter,
		limit:     2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_katie,
	})

	fmt.Println("Test: WHERE ClassYear in (Freshman, Sophomore), LIMIT 2")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "ClassYear", "in", []interface{}{"Freshman", "Sophomore"})
	args = FindArgs{
		andFilter: filter,
		limit:     2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
	})

	/* ------------------------------------------------------------ */

	// WHERE + ORDER BY

	fmt.Println("Test: WHERE IsEnrolled != false, ORDER BY FullName ASC")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsEnrolled", "neq", false)
	orderBy = new(OrderBy)
	addOrder(orderBy, "FullName", "ASC")
	args = FindArgs{
		andFilter: filter,
		orderBy:   *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_albert,
		user_nick,
		user_will,
	})

	/* ------------------------------------------------------------ */

	// LIMIT + ORDER BY

	fmt.Println("Test: ORDER BY IsEnrolled ASC, Age DESC, LIMIT 4")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "IsEnrolled", "ASC")
	addOrder(orderBy, "Age", "DESC")
	args = FindArgs{
		orderBy: *orderBy,
		limit:   4,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_katie,
		user_shannon,
		user_albert,
		user_will,
	})

	/* ------------------------------------------------------------ */

	// PROJECT + WHERE + ORDER BY + LIMIT

	fmt.Println("Test: PROJECT ClassYear, Age, WHERE AGE > 18 and AGE <= 30, ORDER BY ClassYear DESC, LIMIT 10")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "ClassYear", "DESC")
	filter = make(Filter)
	addFilter(filter, "Age", "gt", 18)
	addFilter(filter, "Age", "leq", 30)
	args = FindArgs{
		projection: []interface{}{"ClassYear", "Age"},
		andFilter:  filter,
		orderBy:    *orderBy,
		limit:      10,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{ClassYear: "Sophomore", Age: 30},
		{ClassYear: "Senior", Age: 20},
		{ClassYear: "Freshman", Age: 20},
	})

	fmt.Println("Test: PROJECT FullName, IsEnrolled, WHERE IsEnrolled = true, ClassYear > Freshman, ORDER BY FullName ASC, LIMIT 2")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "FullName", "ASC")
	filter = make(Filter)
	addFilter(filter, "IsEnrolled", "eq", true)
	addFilter(filter, "ClassYear", "gt", "Freshman")
	args = FindArgs{
		projection: []interface{}{"FullName", "IsEnrolled"},
		andFilter:  filter,
		orderBy:    *orderBy,
		limit:      2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Albert", IsEnrolled: true},
		{FullName: "Will", IsEnrolled: true},
	})

	fmt.Println("Test: PROJECT FullName, ClassYear, WHERE Age >= 20, Name not in (Katie), ORDER BY FullName DESC, LIMIT 2")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "FullName", "DESC")
	filter = make(Filter)
	addFilter(filter, "Age", "geq", 20)
	addFilter(filter, "FullName", "nin", []interface{}{"Katie"})
	args = FindArgs{
		projection: []interface{}{"FullName", "ClassYear"},
		andFilter:  filter,
		orderBy:    *orderBy,
		limit:      2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Will", ClassYear: "Senior"},
		{FullName: "Shannon", ClassYear: "Freshman"},
	})
}

func TestDelete(t *testing.T) {
	fmt.Println(">>> DELETE TESTS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Delete Will's Row")
	filter := make(Filter)
	addFilter(filter, "FullName", "eq", "Will")
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	rows_deleted := db.Delete(&User{}, args)
	helperTestIntEquality(t, rows_deleted, 1)

	results := []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
	})

	/* ------------------------------------------------------------ */

	db.Create(&user_will)

	fmt.Println("Test: Delete Freshman Rows")
	filter = make(Filter)
	addFilter(filter, "ClassYear", "eq", "Freshman")
	args = DeleteOrUpdateArgs{
		andFilter: filter,
	}
	rows_deleted = db.Delete(&User{}, args)
	helperTestIntEquality(t, rows_deleted, 2)

	results = []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		user_will,
	})

	/* ------------------------------------------------------------ */

	db.Create(&user_shannon)
	db.Create(&user_nick)

	fmt.Println("Test: Delete All Rows")
	filter = make(Filter)
	args = DeleteOrUpdateArgs{
		andFilter: filter,
	}
	rows_deleted = db.Delete(&User{}, args)
	helperTestIntEquality(t, rows_deleted, 3)

	results = []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{})

	/* ------------------------------------------------------------ */

	db.Create(&user_shannon)
	db.Create(&user_nick)
	db.Create(&user_will)

	fmt.Println("Test: Delete No Rows")
	filter = make(Filter)
	addFilter(filter, "Age", "lt", 0)
	args = DeleteOrUpdateArgs{
		andFilter: filter,
	}
	rows_deleted = db.Delete(&User{}, args)
	helperTestIntEquality(t, rows_deleted, 0)

	results = []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		user_shannon,
		user_nick,
		user_will,
	})
}

func TestUpdateOne(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: 1 ROW <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Update Will's Row")
	filter := make(Filter)
	addFilter(filter, "FullName", "eq", "Will")
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	updates := make(Updates)
	addUpdate(updates, "FullName", "Katie")
	addUpdate(updates, "Age", 15)

	rows_updated := db.Update(&User{}, args, updates)
	helperTestIntEquality(t, rows_updated, 1)

	results := []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
		{FullName: "Katie", ClassYear: "Senior", Age: 15},
	})
}

func TestUpdateTwo(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: 2 ROWS <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Update Shannon & Will's Row")
	filter := make(Filter)
	addFilter(filter, "Age", "gt", 18)
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	updates := make(Updates)
	addUpdate(updates, "ClassYear", "Sophomore")
	addUpdate(updates, "Age", 21)

	rows_updated := db.Update(&User{}, args, updates)
	helperTestIntEquality(t, rows_updated, 2)

	results := []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		user_nick,
		{FullName: "Shannon", ClassYear: "Sophomore", Age: 21},
		{FullName: "Will", ClassYear: "Sophomore", Age: 21},
	})
}

func TestUpdateAll1(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: ALL <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Update All Rows")
	filter := make(Filter)
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	updates := make(Updates)
	addUpdate(updates, "ClassYear", "Sophomore")

	rows_updated := db.Update(&User{}, args, updates)
	helperTestIntEquality(t, rows_updated, 3)

	results := []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		{FullName: "Nick", ClassYear: "Sophomore", Age: 10},
		{FullName: "Shannon", ClassYear: "Sophomore", Age: 20},
		{FullName: "Will", ClassYear: "Sophomore", Age: 20},
	})
}

func TestUpdateAll2(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: ALL <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Update All Rows")
	filter := make(Filter)
	addFilter(filter, "Age", "gt", 1)
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	updates := make(Updates)
	addUpdate(updates, "ClassYear", "Sophomore")

	rows_updated := db.Update(&User{}, args, updates)
	helperTestIntEquality(t, rows_updated, 3)

	results := []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		{FullName: "Nick", ClassYear: "Sophomore", Age: 10},
		{FullName: "Shannon", ClassYear: "Sophomore", Age: 20},
		{FullName: "Will", ClassYear: "Sophomore", Age: 20},
	})
}

func TestUpdateNone(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: NONE <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Update No Rows")
	filter := make(Filter)
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	addFilter(filter, "ClassYear", "eq", "Junior")
	updates := make(Updates)
	addUpdate(updates, "FullName", "Boo")

	rows_updated := db.Update(&User{}, args, updates)
	helperTestIntEquality(t, rows_updated, 0)

	results := []User{}
	db.Find(&results, FindArgs{})
	helperTestEquality(t, results, []User{
		user_nick,
		user_shannon,
		user_will,
	})
}

func TestUpdateBadField(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: BAD FIELD <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	db.Create(&user_nick)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Field Doesn't Exist")
	filter := make(Filter)
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	updates := make(Updates)
	addUpdate(updates, "FakeField", "")

	helperTestPanic(t, func() {
		db.Update(&User{}, args, updates)
	})
}

func TestUpdateBadType(t *testing.T) {
	fmt.Println(">>> UPDATE TEST: BAD TYPE <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}
	db.Create(&user_nick)

	/* ------------------------------------------------------------ */

	fmt.Println("Test: Invalid Field Value")
	filter := make(Filter)
	args := DeleteOrUpdateArgs{
		andFilter: filter,
	}
	updates := make(Updates)
	addUpdate(updates, "ClassYear", 0)

	helperTestPanic(t, func() {
		db.Update(&User{}, args, updates)
	})
}

func TestInvalidTable(t *testing.T) {
	fmt.Println(">>> INVALID TABLE TEST <<<")
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10}

	db.Create(&user_nick)

	/* ------------------------------------------------------------ */

	type Bad struct{}
	args := DeleteOrUpdateArgs{}
	updates := make(Updates)

	helperTestPanic(t, func() {
		db.Delete(&Bad{}, args)
	})

	helperTestPanic(t, func() {
		db.Update(&Bad{}, args, updates)
	})
}

/* ------------------------------------------------------------ */
/* VIDEO DEMO FUNCTIONS                                         */
/* ------------------------------------------------------------ */

func showResult(results []User) {
	fmt.Printf("\n%-10v | %-10v | %-5v | %-10v \n", "FullName", "ClassYear", "Age", "IsEnrolled")
	fmt.Printf("--------------------------------------------\n")
	for _, res := range results {
		fmt.Printf("%-10v | %-10v | %-5d | %-10v\n", res.FullName, res.ClassYear, res.Age, res.IsEnrolled)
	}
	fmt.Println()
}

func populateVideoDemoDb() DB {
	/*
		// User model
		type User struct {
			FullName  string
			Age       int
			ClassYear string
			IsEnrolled    bool
		}
	*/

	// set up database connection
	conn := connectSQL()
	createUserTable(conn)
	db := NewDB(conn)

	// create dummy data with User model
	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10, IsEnrolled: true}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20, IsEnrolled: false}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20, IsEnrolled: true}
	user_katie := User{FullName: "Katie", ClassYear: "Sophomore", Age: 30, IsEnrolled: false}
	user_albert := User{FullName: "Albert", ClassYear: "Senior", Age: 40, IsEnrolled: true}

	// insert dummy data into user table
	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)
	db.Create(&user_katie)
	db.Create(&user_albert)

	return db
}

func TestVideoDemo1(t *testing.T) {
	db := populateVideoDemoDb()
	defer db.Close()

	// demonstration of Find with projection on Age and ClassYear columns
	results := []User{}
	args := FindArgs{
		projection: []interface{}{"Age", "ClassYear"},
	}
	db.Find(&results, args)
	showResult(results)
}

func TestVideoDemo2(t *testing.T) {
	db := populateVideoDemoDb()
	defer db.Close()

	// demonstration of Find with filter conditions: FullName not in (Nick, Will),
	// Age in (20, 30, 40), IsEnrolled = true
	results := []User{}

	filter := make(Filter)
	addFilter(filter, "FullName", "nin", []interface{}{"Nick", "Will"})
	addFilter(filter, "Age", "in", []interface{}{20, 30, 40})
	addFilter(filter, "IsEnrolled", "eq", true)

	args := FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	// check that the returned User is albert
	showResult(results)
}

func TestVideoDemo3(t *testing.T) {
	db := populateVideoDemoDb()
	defer db.Close()

	// demonstration of Find with primary ordering of ClassYear ascending,
	// and secondary ordering of Age descending
	// demonstration of Find with returned rows limit of 3
	results := []User{}
	orderBy := new(OrderBy)
	addOrder(orderBy, "Age", "ASC")
	addOrder(orderBy, "FullName", "DESC")
	args := FindArgs{
		orderBy: *orderBy,
		limit:   3,
	}
	db.Find(&results, args)
	showResult(results)
}

func TestVideoDemo4(t *testing.T) {
	db := populateVideoDemoDb()
	defer db.Close()

	// demonstration of Find with projection, filtering, ordering, and limit:
	// Age >= 20, FullName not in (Katie)
	// Order by FullName, descending
	// Projection for FullName and ClassYear fields
	// Limit to 2 returned rows
	results := []User{}

	orderBy := new(OrderBy)
	addOrder(orderBy, "FullName", "DESC")

	filter := make(Filter)
	addFilter(filter, "Age", "geq", 20)
	addFilter(filter, "FullName", "nin", []interface{}{"Katie"})

	args := FindArgs{
		projection: []interface{}{"FullName", "ClassYear"},
		andFilter:  filter,
		orderBy:    *orderBy,
		limit:      2,
	}
	db.Find(&results, args)
	showResult(results)
}

func TestVideoDemo5(t *testing.T) {
	db := populateVideoDemoDb()
	defer db.Close()

	// demonstration of Delete on rows where
	// FullName in (Will, Katie, Albert), ClassYear = "Senior", and IsEnrolled = true
	filter := make(Filter)
	addFilter(filter, "FullName", "in", []interface{}{"Will", "Katie", "Albert"})
	addFilter(filter, "ClassYear", "eq", "Senior")
	addFilter(filter, "IsEnrolled", "eq", true)

	delete_args := DeleteOrUpdateArgs{
		andFilter: filter,
	}

	rows_deleted := db.Delete(&User{}, delete_args)
	fmt.Println("Rows Deleted:", rows_deleted) // should be 2

	// check that rows were properly updated by fetching all rows using Find without a filter
	results := []User{}
	db.Find(&results, FindArgs{})
	showResult(results)
}

func TestVideoDemo6(t *testing.T) {
	db := populateVideoDemoDb()
	defer db.Close()

	// demonstration of Update on rows where Age > 18:
	// change ClassYear to "Sophomore" and Age to 21
	filter := make(Filter)

	addFilter(filter, "Age", "gt", 18)
	addFilter(filter, "Age", "lt", 40)
	update_args := DeleteOrUpdateArgs{
		andFilter: filter,
	}

	updates := make(Updates)
	addUpdate(updates, "ClassYear", "Sophomore")
	addUpdate(updates, "Age", 21)

	rows_updated := db.Update(&User{}, update_args, updates)
	fmt.Println("Rows Updated:", rows_updated) // should be 3

	// check that rows were properly updated by fetching all rows using Find without a filter
	results := []User{}
	db.Find(&results, FindArgs{})
	showResult(results)
}
