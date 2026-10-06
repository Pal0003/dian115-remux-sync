package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func candidate(id int64, count int, size int64) Candidate {
	c := Candidate{Media: Media{Type: "tv", TMDB: 100, Title: "示例", Year: 2024}, ResourceID: 10, ShareID: id, Kind: "115", Remux: true, Bytes: size, CoverageKnown: true, Points: 10}
	for i := 1; i <= count; i++ {
		c.Episodes = append(c.Episodes, Episode{1, i})
	}
	return c
}
func snapshot(cs ...Candidate) Snapshot {
	return Snapshot{CatalogComplete: true, Candidates: cs, RequiredLibraries: []string{"movies", "series"}, Libraries: []Library{{ID: "movies", Complete: true}, {ID: "series", Complete: true}}}
}
func TestPlanCompletionBeforeSize(t *testing.T) {
	a, b, c := candidate(1, 8, 1000), candidate(2, 10, 100), candidate(3, 10, 200)
	b.Episodes = append(b.Episodes, b.Episodes...) // repeated labels aren't new episodes
	p, e := Plan(snapshot(a, b, c))
	if e != nil || len(p.Selected) != 1 || p.Selected[0].ShareID != 3 {
		t.Fatalf("%+v %v", p, e)
	}
	p, e = Plan(snapshot(c, b, a))
	if e != nil || p.Selected[0].ShareID != 3 {
		t.Fatal("order-dependent plan")
	}
}
func TestIncompleteScanFailsClosed(t *testing.T) {
	s := snapshot(candidate(1, 8, 100))
	s.CatalogComplete = false
	if _, e := Plan(s); e == nil {
		t.Fatal("partial catalog accepted")
	}
	s.CatalogComplete = true
	s.Libraries[1].Complete = false
	if _, e := Plan(s); e == nil {
		t.Fatal("partial Emby snapshot accepted")
	}
}
func TestCrossLibraryAndAmbiguousIdentity(t *testing.T) {
	c := candidate(1, 8, 100)
	s := snapshot(c)
	s.Libraries[0].Items = []Media{c.Media}
	p, _ := Plan(s)
	if len(p.Selected) != 0 || len(p.Skipped) != 1 {
		t.Fatal("cross-library duplicate")
	}
	s.Libraries[0].Items[0].TMDB = 0
	p, _ = Plan(s)
	if len(p.Selected) != 0 || len(p.Held) != 1 {
		t.Fatal("ambiguous identity accepted")
	}
	s.Libraries[0].Items[0].TMDB = 200
	p, _ = Plan(s)
	if len(p.Selected) != 1 {
		t.Fatal("different TMDB identities conflated")
	}
}
func TestUnknownCoverageHoldsWholeGroup(t *testing.T) {
	a, b := candidate(1, 8, 100), candidate(2, 10, 200)
	b.CoverageKnown = false
	p, _ := Plan(snapshot(a, b))
	if len(p.Selected) != 0 || len(p.Held) != 2 {
		t.Fatal("selected despite unknown competitor")
	}
}
func TestSerialBatchAndIngestion(t *testing.T) {
	a, b := candidate(1, 8, 100), candidate(2, 9, 200)
	b.Media.TMDB = 200
	q, _ := NewQueue("run", PlanResult{Selected: []Candidate{a, b}})
	for _, want := range []string{"check_library", "unlock", "receive", "organize", "check_library", "unlock", "receive", "organize", "refresh_library", "verify_ingestion", "verify_ingestion"} {
		c, e := q.Prepare()
		if e != nil || c == nil || c.Kind != want {
			t.Fatalf("want %s: %+v %v", want, c, e)
		}
		if _, e = q.Prepare(); e == nil {
			t.Fatal("advanced while pending")
		}
		status := "success"
		if want == "check_library" {
			status = "absent"
		}
		if e = q.Resolve(c.ID, Outcome{Status: status, Reference: "opaque-job-ref"}); e != nil {
			t.Fatal(e)
		}
	}
	if c, e := q.Prepare(); c != nil || e != nil {
		t.Fatal("queue not complete")
	}
	if len(q.ActiveMedia()) != 0 {
		t.Fatal("completed title active")
	}
}
func TestSlowIngestionDoesNotRepeatTransferOrBlockOtherTitles(t *testing.T) {
	a, b := candidate(1, 8, 100), candidate(2, 9, 200)
	b.Media.TMDB = 200
	q, _ := NewQueue("run", PlanResult{Selected: []Candidate{a, b}})
	q.Refreshed = true
	q.Tasks[0].State = "organized"
	q.Tasks[1].State = "organized"
	c, _ := q.Prepare()
	if c.Index != 0 {
		t.Fatal(c)
	}
	q.Resolve(c.ID, Outcome{Status: "absent"})
	c, _ = q.Prepare()
	if c.Index != 1 || c.Kind != "verify_ingestion" {
		t.Fatal(c)
	}
	q.Resolve(c.ID, Outcome{Status: "success"})
	c, _ = q.Prepare()
	if c.Kind != "verify_ingestion" || c.Index != 0 {
		t.Fatal("slow title re-transferred")
	}
	if len(q.ActiveMedia()) != 1 {
		t.Fatal("pending title missing from dedup")
	}
}

type memoryStore struct {
	data     []byte
	version  int
	failSave bool
}

func (s *memoryStore) Load() (Queue, string, error) {
	var q Queue
	e := json.Unmarshal(s.data, &q)
	return q, fmt.Sprint(s.version), e
}
func (s *memoryStore) Save(q Queue, etag string) (string, error) {
	if s.failSave || etag != fmt.Sprint(s.version) {
		return "", errors.New("CAS conflict")
	}
	s.data, _ = json.Marshal(q)
	s.version++
	return fmt.Sprint(s.version), nil
}

type fakeAdapter struct {
	execute, reconcile int
	unknown            bool
}

func (a *fakeAdapter) Execute(c Command, q Queue) (Outcome, error) {
	a.execute++
	if a.unknown {
		return Outcome{Status: "unknown"}, nil
	}
	return Outcome{Status: "absent"}, nil
}
func (a *fakeAdapter) Reconcile(c Command, q Queue) (Outcome, error) {
	a.reconcile++
	return Outcome{Status: "absent"}, nil
}
func TestPersistBeforeIOAndRecoverWithoutNewRequest(t *testing.T) {
	q, _ := NewQueue("run", PlanResult{Selected: []Candidate{candidate(1, 8, 100)}})
	data, _ := json.Marshal(q)
	s := &memoryStore{data: data, failSave: true}
	a := &fakeAdapter{unknown: true}
	r := Runner{s, a}
	if r.Step() == nil || a.execute != 0 {
		t.Fatal("executed before durable save")
	}
	s.failSave = false
	if e := r.Step(); e != nil {
		t.Fatal(e)
	}
	saved, _, _ := s.Load()
	if saved.Pending == nil {
		t.Fatal("uncertain outcome forgotten")
	}
	if e := r.Step(); e != nil {
		t.Fatal(e)
	}
	if a.execute != 1 || a.reconcile != 1 {
		t.Fatal("recovery repeated operation")
	}
	saved, _, _ = s.Load()
	if saved.Tasks[0].State != "absent" || saved.Pending != nil {
		t.Fatal("recovery did not advance")
	}
}
func TestRejectStaleResult(t *testing.T) {
	q, _ := NewQueue("run", PlanResult{Selected: []Candidate{candidate(1, 8, 100)}})
	q.Prepare()
	if e := q.Resolve("stale", Outcome{Status: "success"}); e == nil {
		t.Fatal("stale callback accepted")
	}
}

func TestInFlightAndUnknownSize(t *testing.T) {
	a, b := candidate(1, 8, 100), candidate(2, 8, 0)
	p, e := Plan(snapshot(a, b))
	if e != nil || len(p.Selected) != 0 || len(p.Held) != 2 {
		t.Fatal("unknown size competitor ignored")
	}
	s := snapshot(a)
	s.InFlight = []Media{a.Media}
	p, e = Plan(s)
	if e != nil || len(p.Selected) != 0 || len(p.Skipped) != 1 {
		t.Fatal("in-flight title queued again")
	}
}
