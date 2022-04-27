package dorm

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

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
		is_male int
	)`)

	if err != nil {
		panic(err)
	}
}

func insertUsers(conn *sql.DB, users []User) {
	for _, uc := range users {
		_, err := conn.Exec(`insert into user
		values
		(?)`, uc.FullName)

		if err != nil {
			panic(err)
		}
	}
}

// var MockUsers = []User{
// 	{FullName: "Test User2"},
// 	{FullName: "Test User1"},
// }

// var MockUsers2 = []User{
// 	{FullName: "Test User3"},
// }

// func TestSuccess(t *testing.T) {
// 	conn := connectSQL()
// 	createUserTable(conn)
// 	insertUsers(conn, MockUsers)

// 	db := NewDB(conn)
// 	defer db.Close()

// 	results := []User{}
// 	// db.Find(&results)

// 	if len(results) != 2 {
// 		t.Errorf("Expected 2 users but found %d", len(results))
// 	}

// 	result := User{}
// 	is_success := db.First(&result)
// 	if !is_success {
// 		t.Errorf("Expected success for First")
// 	}
// 	if result.FullName != "Test User2" {
// 		t.Errorf("Expected Test User2 but instead found %v", result.FullName)
// 	}

// 	results = []User{}
// 	insertUsers(conn, MockUsers2)
// 	db.Find(&results)

// 	if len(results) != 3 {
// 		t.Errorf("Expected 3 users but found %d", len(results))
// 	}

// 	result = User{}
// 	is_success = db.First(&result)
// 	if !is_success {
// 		t.Errorf("Expected success for First")
// 	}
// 	if result.FullName != "Test User2" {
// 		t.Errorf("Expected Test User2 but instead found %v", result.FullName)
// 	}

// }

// func TestFailure(t *testing.T) {
// 	conn := connectSQL()
// 	createUserTable(conn)

// 	db := NewDB(conn)
// 	defer db.Close()

// 	results := []User{}
// 	db.Find(&results)

// 	if len(results) != 0 {
// 		t.Errorf("Expected 0 users but found %d", len(results))
// 	}

// 	result := User{}
// 	is_success := db.First(&result)
// 	if is_success {
// 		t.Errorf("Expected failure for First")
// 	}

// }

// func TestCreate(t *testing.T) {
// 	conn := connectSQL()
// 	createUserTable(conn)

// 	db := NewDB(conn)
// 	defer db.Close()

// 	db.Create(&User{FullName: "Nick"})

// 	results := []User{}
// 	db.Find(&results)

// 	if len(results) != 1 {
// 		t.Errorf("Expected 1 user but found %d", len(results))
// 	}

// 	result := User{}
// 	is_success := db.First(&result)
// 	if !is_success {
// 		t.Errorf("Expected success for First")
// 	}
// 	if result.FullName != "Nick" {
// 		t.Errorf("Expected Nick but instead found %v", result.FullName)
// 	}

// 	db.Create(&User{FullName: "Shan"})

// 	results = []User{}
// 	db.Find(&results)

// 	if len(results) != 2 {
// 		t.Errorf("Expected 2 users but found %d", len(results))
// 	}

// 	result = User{}
// 	is_success = db.First(&result)
// 	if !is_success {
// 		t.Errorf("Expected success for First")
// 	}
// 	if result.FullName != "Nick" {
// 		t.Errorf("Expected Nick but instead found %v", result.FullName)
// 	}
// }

type Post struct {
	ID     int64 `dorm:"primary_key"`
	Author string
	Posted time.Time
	Likes  int
	Body   string
}

type User struct {
	FullName  string
	Age 	  int
	ClassYear string
	IsMale 	  bool
}

func helperTestEquality(t *testing.T, results []User, expected []User) {
	if (len(results) != len(expected)) {
		t.Errorf("Expected %v rows but instead found %v rows",  len(expected), len(results))
	}
	for i, result := range(results) {
		if (result.FullName != expected[i].FullName) {
			t.Errorf("Expected %v but instead found %v",  expected[i].FullName, result.FullName)
		}
		if (result.Age != expected[i].Age) {
			t.Errorf("Expected %v but instead found %v",  expected[i].Age, result.Age)
		}
		if (result.ClassYear != expected[i].ClassYear) {
			t.Errorf("Expected %v but instead found %v",  expected[i].ClassYear, result.ClassYear)
		}
	}
}

func helperTestPanic(t *testing.T, theFunc func() ) {
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

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10, IsMale: true}
	user_shannon := User{FullName: "Shannon", ClassYear: "Senior", Age: 20, IsMale: false}

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
	
	fmt.Println("Test: Get IsMale = true, Only Shannon")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsMale", "neq", true)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
	})

	fmt.Println("Test: Get IsMale = false, Only Shannon")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsMale", "eq", false)
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_shannon,
	})

	fmt.Println("Test: Get IsMale = true and FullName = Nicj, Only Nick")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsMale", "eq", true)
	addFilter(filter, "FullName", "gt", "Nicj")
	args = FindArgs{
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
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

	user_nick := User{FullName: "Nick", ClassYear: "Freshman", Age: 10, IsMale: true}
	user_shannon := User{FullName: "Shannon", ClassYear: "Freshman", Age: 20, IsMale: false}
	user_will := User{FullName: "Will", ClassYear: "Senior", Age: 20, IsMale: true}
	user_katie := User{FullName: "Katie", ClassYear: "Sophomore", Age: 30, IsMale: false}
	user_albert := User{FullName: "Albert", ClassYear: "Senior", Age: 40, IsMale: true}

	db.Create(&user_nick)
	db.Create(&user_shannon)
	db.Create(&user_will)
	db.Create(&user_katie)
	db.Create(&user_albert)

	/* ------------------------------------------------------------ */
	
	// PROJECT + WHERE

	fmt.Println("Test: PROJECT FullName, IsMale, WHERE ClassYear != Freshman, Age > 20")
	results := []User{}
	filter := make(Filter)
	addFilter(filter, "ClassYear", "neq", "Freshman")
	addFilter(filter, "Age", "gt", 20)
	args := FindArgs{
		projection: []interface{}{"FullName", "IsMale"},
		andFilter: filter,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Katie", IsMale: false},
		{FullName: "Albert", IsMale: true},
	})

	// add IN operator
	fmt.Println("Test: PROJECT Age, ClassYear, WHERE IsMale = true")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsMale", "eq", true)
	args = FindArgs{
		projection: []interface{}{"Age", "ClassYear"},
		andFilter: filter,
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
		limit: 2,
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
		orderBy: *orderBy,
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
		limit: 2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_nick,
		user_katie,
	})

	/* ------------------------------------------------------------ */
	
	// WHERE + ORDER BY

	fmt.Println("Test: WHERE IsMale != false, ORDER BY FullName ASC")
	results = []User{}
	filter = make(Filter)
	addFilter(filter, "IsMale", "neq", false)
	orderBy = new(OrderBy)
	addOrder(orderBy, "FullName", "ASC")
	args = FindArgs{
		andFilter: filter,
		orderBy: *orderBy,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		user_albert,
		user_nick,
		user_will,
	})

	/* ------------------------------------------------------------ */
	
	// LIMIT + ORDER BY

	fmt.Println("Test: ORDER BY IsMale ASC, Age DESC, LIMIT 4")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "IsMale", "ASC")
	addOrder(orderBy, "Age", "DESC")
	args = FindArgs{
		orderBy: *orderBy,
		limit: 4,
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
		andFilter: filter,
		orderBy: *orderBy,
		limit: 10,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{ClassYear: "Sophomore", Age: 30},
		{ClassYear: "Senior", Age: 20},
		{ClassYear: "Freshman", Age: 20},
	})

	fmt.Println("Test: PROJECT FullName, IsMale, WHERE IsMale = true, ClassYear > Freshman, ORDER BY FullName ASC, LIMIT 2")
	results = []User{}
	orderBy = new(OrderBy)
	addOrder(orderBy, "FullName", "ASC")
	filter = make(Filter)
	addFilter(filter, "IsMale", "eq", true)
	addFilter(filter, "ClassYear", "gt", "Freshman")
	args = FindArgs{
		projection: []interface{}{"FullName", "IsMale"},
		andFilter: filter,
		orderBy: *orderBy,
		limit: 2,
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Albert", IsMale: true},
		{FullName: "Will", IsMale: true},
	})
}

// func TestCustom(t *testing.T) {
// 	// copied from main.go
// 	conn, err := sql.Open("sqlite3", "file:test.db?mode=memory")
// 	if err != nil {
// 		panic(err)
// 	}
// 	_, err = conn.Exec(`create table post (
// 		id integer primary key autoincrement,
// 		author text,
// 		posted timestamp,
// 		likes number,
// 		body text
// 	)`)

// 	if err != nil {
// 		panic(err)
// 	}

// 	orm := NewDB(conn)

// 	// Create a new post
// 	post1 := &Post{
// 		ID:     100,
// 		Author: "alevy",
// 		Posted: time.Now(),
// 		Likes:  0,
// 		Body:   "Hello fellow kids! This post will surely be viral",
// 	}

// 	// Insert the record into the database
// 	orm.Create(post1)
// 	fmt.Println(*post1) // make sure ID is changed

// 	// Get and display all posts
// 	allPosts := []Post{}
// 	orm.Find(&allPosts)

// 	for _, post := range allPosts {
// 		fmt.Printf("%s said: %s\n", post.Author, post.Body)
// 	}
// }
