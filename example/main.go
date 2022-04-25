package main

import (
	"time"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"

	"cos316.princeton.edu/assignment4/dorm"
)

type Post struct {
	ID     int64  `dorm:"primary_key"`
	Author string
	Posted time.Time
	Likes  int
	Body   string
}

func main() {
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

	orm := dorm.NewDB(conn)

	// Create a new post
	post1 := &Post{
		Author: "alevy",
		Posted: time.Now(),
		Likes: 0,
		Body: "Hello fellow kids! This post will surely be viral",
	}

	// Insert the record into the database
	orm.Create(post1)

	// Get and display all posts
	allPosts := []Post{}
	orm.Find(&allPosts)

	for _, post := range allPosts {
	    fmt.Printf("%s said: %s\n", post.Author, post.Body)
	}
}

