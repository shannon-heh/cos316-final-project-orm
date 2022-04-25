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
		full_name text
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
}

var MockUsers = []User{
	{FullName: "Test User2"},
	{FullName: "Test User1"},
}

var MockUsers2 = []User{
	{FullName: "Test User3"},
}

func TestSuccess(t *testing.T) {
	conn := connectSQL()
	createUserTable(conn)
	insertUsers(conn, MockUsers)

	db := NewDB(conn)
	defer db.Close()

	results := []User{}
	db.Find(&results)

	if len(results) != 2 {
		t.Errorf("Expected 2 users but found %d", len(results))
	}

	result := User{}
	is_success := db.First(&result)
	if !is_success {
		t.Errorf("Expected success for First")
	}
	if result.FullName != "Test User2" {
		t.Errorf("Expected Test User2 but instead found %v", result.FullName)
	}

	results = []User{}
	insertUsers(conn, MockUsers2)
	db.Find(&results)

	if len(results) != 3 {
		t.Errorf("Expected 3 users but found %d", len(results))
	}

	result = User{}
	is_success = db.First(&result)
	if !is_success {
		t.Errorf("Expected success for First")
	}
	if result.FullName != "Test User2" {
		t.Errorf("Expected Test User2 but instead found %v", result.FullName)
	}

}

func TestFailure(t *testing.T) {
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	results := []User{}
	db.Find(&results)

	if len(results) != 0 {
		t.Errorf("Expected 0 users but found %d", len(results))
	}

	result := User{}
	is_success := db.First(&result)
	if is_success {
		t.Errorf("Expected failure for First")
	}

}

func TestCreate(t *testing.T) {
	conn := connectSQL()
	createUserTable(conn)

	db := NewDB(conn)
	defer db.Close()

	db.Create(&User{FullName: "Nick"})

	results := []User{}
	db.Find(&results)

	if len(results) != 1 {
		t.Errorf("Expected 1 user but found %d", len(results))
	}

	result := User{}
	is_success := db.First(&result)
	if !is_success {
		t.Errorf("Expected success for First")
	}
	if result.FullName != "Nick" {
		t.Errorf("Expected Nick but instead found %v", result.FullName)
	}

	db.Create(&User{FullName: "Shan"})

	results = []User{}
	db.Find(&results)

	if len(results) != 2 {
		t.Errorf("Expected 2 users but found %d", len(results))
	}

	result = User{}
	is_success = db.First(&result)
	if !is_success {
		t.Errorf("Expected success for First")
	}
	if result.FullName != "Nick" {
		t.Errorf("Expected Nick but instead found %v", result.FullName)
	}
}

type Post struct {
	ID     int64 `dorm:"primary_key"`
	Author string
	Posted time.Time
	Likes  int
	Body   string
}

func TestCustom(t *testing.T) {
	// copied from main.go
	conn, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec(`create table post (
		id integer primary key autoincrement,
		author text,
		posted timestamp,
		likes number,
		body text
	)`)

	if err != nil {
		panic(err)
	}

	orm := NewDB(conn)

	// Create a new post
	post1 := &Post{
		ID:     100,
		Author: "alevy",
		Posted: time.Now(),
		Likes:  0,
		Body:   "Hello fellow kids! This post will surely be viral",
	}

	// Insert the record into the database
	orm.Create(post1)
	fmt.Println(*post1) // make sure ID is changed

	// Get and display all posts
	allPosts := []Post{}
	orm.Find(&allPosts)

	for _, post := range allPosts {
		fmt.Printf("%s said: %s\n", post.Author, post.Body)
	}
}
