package requeue // import "github.com/jfontan/requeue"

import (
	"database/sql"
	"io/ioutil"
	"strings"

	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"
	kallax "gopkg.in/src-d/go-kallax.v1"
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
