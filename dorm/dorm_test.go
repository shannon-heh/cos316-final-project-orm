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
		class_year text
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

type User struct {
	FullName string
	Age int
	ClassYear string
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

func helperTestEquality(t *testing.T, results []User, expected []User) {
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

	fmt.Println("Test: Only FullName")
	results := []User{}
	args := FindArgs{
		projection: []string{"FullName"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Nick"},
		{FullName: "Shannon"},
	})
	
	fmt.Println("Test: Only ClassYear and Age")
	results = []User{}
	args = FindArgs{
		projection: []string{"ClassYear", "Age"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{ClassYear: "Freshman", Age: 10},
		{ClassYear: "Senior", Age: 20},
	})

	fmt.Println("Test: Only FullName and Age")
	results = []User{}
	args = FindArgs{
		projection: []string{"FullName", "Age"},
	}
	db.Find(&results, args)
	helperTestEquality(t, results, []User{
		{FullName: "Nick", Age: 10},
		{FullName: "Shannon", Age: 20},
	})

	fmt.Println("Test: All Results - Empty Projection Array")
	results = []User{}
	args = FindArgs{
		projection: []string{},
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

	fmt.Println(">>> passed! <<<")
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
