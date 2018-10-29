package main

import (
	"database/sql"

	"github.com/jfontan/requeue"

	_ "github.com/lib/pq" // load postgres db driver
	"github.com/sanity-io/litter"
	queue "gopkg.in/src-d/go-queue.v1"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

const (
	dbConn    = "postgres://testing:testing@localhost/testing?sslmode=disable"
	queueConn = "amqp://localhost:5672"
	queueName = "borges"
	file      = "list.txt"
)

func main() {
	db, err := sql.Open("postgres", dbConn)
	if err != nil {
		panic(err)
	}

	b, err := queue.NewBroker(queueConn)
	if err != nil {
		panic(err)
	}

	q, err := b.Queue(queueName)
	if err != nil {
		panic(err)
	}

	checker, err := requeue.NewSivaChecker(file, db, q)
	if err != nil {
		panic(err)
	}

	err = checker.Check()
	if err != nil {
		panic(err)
	}

	litter.Dump(checker.List())
}
