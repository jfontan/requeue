package main // import "github.com/jfontan/requeue"

import (
	"database/sql"
	"io/ioutil"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/sanity-io/litter"
	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"
	kallax "gopkg.in/src-d/go-kallax.v1"

	_ "github.com/lib/pq"
)

const (
	connectionString = "postgres://testing:testing@localhost/testing?sslmode=disable"
	file             = "list-broken.txt"
)

type Set struct {
	l map[string]struct{}
	m sync.RWMutex
}

func NewSet() *Set {
	return &Set{
		l: make(map[string]struct{}),
	}
}

func (s *Set) Add(name string) {
	s.m.Lock()
	defer s.m.Unlock()

	s.l[name] = struct{}{}
}

func (s *Set) Contains(name string) bool {
	s.m.RLock()
	defer s.m.RUnlock()

	_, ok := s.l[name]
	return ok
}

func (s *Set) List() []string {
	l := make([]string, len(s.l))

	s.m.RLock()

	var i int
	for k := range s.l {
		l[i] = k
		i++
	}

	s.m.RUnlock()

	sort.Strings(l)
	return l
}

func loadHashes(file string) (map[string]struct{}, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	list := strings.Split(strings.TrimSpace(string(data)), "\n")
	hashes := make(map[string]struct{}, len(list))

	for _, h := range list {
		if h != "" {
			hashes[strings.TrimSpace(strings.ToLower(h))] = struct{}{}
		}
	}

	return hashes, nil
}

type RepoInit struct {
	Repo string
	Init string
}

type SivaChecker struct {
	repos  *Set
	hashes map[string]struct{}
	c      chan RepoInit

	db    *sql.DB
	store borges.RepositoryStore
}

func NewSivaChecker(file string, db *sql.DB) (*SivaChecker, error) {
	hashes, err := loadHashes(file)
	if err != nil {
		return nil, err
	}

	store := storage.FromDatabase(db)

	return &SivaChecker{
		repos:  NewSet(),
		hashes: hashes,
		c:      make(chan RepoInit),
		db:     db,
		store:  store,
	}, nil
}

func (s *SivaChecker) Worker() {
	for r := range s.c {
		println("row", r.Repo, r.Init)

		if s.repos.Contains(r.Repo) {
			continue
		}

		_, ok := s.hashes[r.Init]
		if ok {
			continue
		}

		s.Requeue(r.Repo)
		s.repos.Add(r.Repo)
	}
}

func (s *SivaChecker) Requeue(id string) error {
	ulid, err := kallax.NewULIDFromText(id)
	if err != nil {
		return err
	}

	r, err := s.store.Get(ulid)
	if err != nil {
		return err
	}

	println("requeue", id, r.Endpoints[0], r.Status)

	return nil
}

func (s *SivaChecker) Chan() chan<- RepoInit {
	return s.c
}

func (s *SivaChecker) List() []string {
	return s.repos.List()
}

func main() {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}

	checker, err := NewSivaChecker(file, db)
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

		sivaChan <- RepoInit{repo, init}
	}

	close(sivaChan)
	wg.Wait()

	litter.Dump(checker.List())
}
