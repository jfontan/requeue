package requeue // import "github.com/jfontan/requeue"

import (
	"database/sql"
	"io/ioutil"
	"runtime"
	"strings"
	"sync"

	"github.com/sanity-io/litter"
	"github.com/satori/go.uuid"
	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"
	kallax "gopkg.in/src-d/go-kallax.v1"
	queue "gopkg.in/src-d/go-queue.v1"
)

const (
	query = "select repository_id, init from repository_references " +
		"group by repository_id, init"
	jobRetries int32 = 5
)

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
	wg     sync.WaitGroup

	db    *sql.DB
	store borges.RepositoryStore

	q queue.Queue
}

func NewSivaChecker(file string, db *sql.DB, q queue.Queue) (*SivaChecker, error) {
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

func (s *SivaChecker) Check() error {
	for i := 0; i < runtime.NumCPU(); i++ {
		s.wg.Add(1)
		go func() {
			s.Worker()

			s.wg.Done()
		}()
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return err
	}

	var repo, init string
	for rows.Next() {
		err = rows.Scan(&repo, &init)
		if err != nil {
			close(s.c)
			return err
		}

		s.c <- RepoInit{Repo: repo, Init: init}
	}

	close(s.c)
	s.wg.Wait()

	return nil
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

	job := &borges.Job{RepositoryID: uuid.UUID(r.ID)}

	qj, err := queue.NewJob()
	if err != nil {
		return err
	}

	qj.Retries = jobRetries
	if err := qj.Encode(job); err != nil {
		return err
	}

	qj.SetPriority(queue.PriorityNormal)

	litter.Dump(qj)

	// return p.queue.Publish(qj)
	return nil
}

func (s *SivaChecker) Chan() chan<- RepoInit {
	return s.c
}

func (s *SivaChecker) List() []string {
	return s.repos.List()
}
