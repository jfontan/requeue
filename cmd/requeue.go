package main

import (
	"database/sql"

	flags "github.com/jessevdk/go-flags"
	"github.com/jfontan/requeue"

	_ "github.com/lib/pq" // load postgres db driver
	queue "gopkg.in/src-d/go-queue.v1"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

const (
	dbConn    = "postgres://testing:testing@localhost/testing?sslmode=disable"
	queueConn = "amqp://localhost:5672"
	queueName = "borges"
	file      = "list.txt"
)

var opts struct {
	Database  string `long:"database" default:"postgres://testing:testing@localhost/testing?sslmode=disable" description:"database connection string"`
	Queue     string `long:"queue" default:"amqp://localhost:5672" description:"rabbitmq connection string"`
	QueueName string `long:"queue-name" default:"borges" description:"queue name to send new jobs"`
	Dry       bool   `long:"dry" description:"do not send jobs or modify database"`

	Args struct {
		File string `positional-arg-name:"list" required:"yes" description:"file name with the list of found hashes"`
	} `positional-args:"yes" required:"yes"`
}

func main() {
	_, err := flags.Parse(&opts)
	if e, ok := err.(*flags.Error); ok {
		if e.Type == flags.ErrHelp {
			return
		}
	}
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", opts.Database)
	if err != nil {
		panic(err)
	}

	b, err := queue.NewBroker(opts.Queue)
	if err != nil {
		panic(err)
	}

	q, err := b.Queue(opts.QueueName)
	if err != nil {
		panic(err)
	}

	println("file", opts.Args.File)
	checker, err := requeue.NewSivaChecker(opts.Args.File, db, q, opts.Dry)
	if err != nil {
		panic(err)
	}

	err = checker.Check()
	if err != nil {
		panic(err)
	}
}
