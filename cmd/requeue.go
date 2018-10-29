package main

import (
	"database/sql"
	"runtime"
	"sync"

	"github.com/jfontan/requeue"

	_ "github.com/lib/pq"
	"github.com/sanity-io/litter"
)

const (
	connectionString = "postgres://testing:testing@localhost/testing?sslmode=disable"
	file             = "list-broken.txt"
)

func main() {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}

	checker, err := requeue.NewSivaChecker(file, db)
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("select repository_id, init from repository_references group by repository_id, init")
	if err != nil {
		panic(err)
	}

	sivaChan := checker.Chan()
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			checker.Worker()

			wg.Done()
		}()
	}

	var repo, init string
	for rows.Next() {
		err = rows.Scan(&repo, &init)
		if err != nil {
			panic(err)
		}

		sivaChan <- requeue.RepoInit{Repo: repo, Init: init}
	}

	close(sivaChan)
	wg.Wait()

	litter.Dump(checker.List())
}
