package requeue // import "github.com/jfontan/requeue"

import (
	"bufio"
	"database/sql"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"
	"gopkg.in/src-d/core-retrieval.v0/model"
	kallax "gopkg.in/src-d/go-kallax.v1"
	log "gopkg.in/src-d/go-log.v1"
	queue "gopkg.in/src-d/go-queue.v1"
)

const (
	query = "select repository_id, init from repository_references " +
		"group by repository_id, init"
	jobRetries int32 = 5
)

func loadHashes(file string) (map[string]struct{}, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	hashes := make(map[string]struct{})

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		t := strings.TrimSpace(strings.ToLower(scanner.Text()))
		t = strings.TrimSuffix(t, ".siva")
		p := strings.Split(t, "/")

		if len(p) > 1 {
			t = p[len(p)-1]
		}

		if t != "" {
			hashes[t] = struct{}{}
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

	dry bool
}

func NewSivaChecker(file string, db *sql.DB, q queue.Queue, dry bool) (*SivaChecker, error) {
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
		q:      q,
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

	log.Infof("querying database")
	start := time.Now()

	rows, err := s.db.Query(query)
	if err != nil {
		return err
	}

	log.With(log.Fields{"duration": time.Since(start)}).
		Infof("database query ended")

	var counter uint64
	var repo, init string
	for rows.Next() {
		err = rows.Scan(&repo, &init)
		if err != nil {
			close(s.c)
			return err
		}

		s.c <- RepoInit{Repo: repo, Init: init}

		counter++
		if counter%10000 == 0 {
			log.With(log.Fields{"counter": counter}).Infof("still working")
		}
	}

	close(s.c)
	s.wg.Wait()

	return nil
}

func (s *SivaChecker) Worker() {
	for r := range s.c {
		if s.repos.Contains(r.Repo) {
			continue
		}

		_, ok := s.hashes[r.Init]
		if ok {
			continue
		}

		err := s.Requeue(r.Repo)
		if err != nil {
			log.With(log.Fields{"id": r.Repo}).Errorf(err, "could not requeue")
		}

		s.repos.Add(r.Repo)
	}
}

func (s *SivaChecker) Requeue(id string) error {
	l := log.With(log.Fields{"id": id})

	ulid, err := kallax.NewULIDFromText(id)
	if err != nil {
		return err
	}

	r, err := s.store.Get(ulid)
	if err != nil {
		return err
	}

	if r.Status != model.Fetched {
		l.Infof("repository not in fetched state")
		return nil
	}

	l.Infof("requeuing job")

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

	if s.dry {
		return nil
	}

	err = s.q.Publish(qj)
	if err != nil {
		return err
	}

	return s.store.SetStatus(r, model.Pending)
}

func (s *SivaChecker) Chan() chan<- RepoInit {
	return s.c
}

func (s *SivaChecker) List() []string {
	return s.repos.List()
}
