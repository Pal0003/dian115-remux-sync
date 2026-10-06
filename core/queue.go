package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

const ScanCron = "0 */6 * * *"

type Task struct {
	Candidate Candidate `json:"candidate"`
	State     string    `json:"state"`
	Reference string    `json:"reference,omitempty"` // opaque adapter reference, never a credential
	Failure   string    `json:"failure,omitempty"`
}
type Command struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Index    int    `json:"index"`
	Revision uint64 `json:"revision"`
}
type Queue struct {
	RunID        string   `json:"run_id"`
	Revision     uint64   `json:"revision"`
	Tasks        []Task   `json:"tasks"`
	Pending      *Command `json:"pending,omitempty"`
	Refreshed    bool     `json:"refreshed"`
	VerifyCursor int      `json:"verify_cursor"`
	// -1 explicitly means unlimited, as authorized by the user.
	PerRunPoints int64 `json:"per_run_points"`
	DailyPoints  int64 `json:"daily_points"`
}
type Outcome struct {
	// unknown is not failure: it retains the pending command for reconciliation.
	Status    string // success, failure, unknown, absent
	Reference string
	Message   string
}

func NewQueue(run string, p PlanResult) (Queue, error) {
	if run == "" {
		return Queue{}, errors.New("run ID is required")
	}
	q := Queue{RunID: run, PerRunPoints: -1, DailyPoints: -1}
	seen := map[string]bool{}
	for _, c := range p.Selected {
		k := mediaKey(c.Media)
		if seen[k] {
			return Queue{}, errors.New("duplicate media in plan")
		}
		seen[k] = true
		q.Tasks = append(q.Tasks, Task{Candidate: c, State: "queued"})
	}
	return q, nil
}

// Prepare only mutates state; it NEVER performs IO. The runner MUST commit the
// returned queue with compare-and-swap before executing this command. If that
// save fails, it must not execute. A recovered Pending command must be reconciled
// with the provider before a retry, retaining the same ID.
func (q *Queue) Prepare() (*Command, error) {
	if q.Pending != nil {
		return nil, errors.New("pending operation must be reconciled")
	}
	if q.RunID == "" {
		return nil, errors.New("invalid queue")
	}
	for i, t := range q.Tasks {
		switch t.State {
		case "queued":
			return q.issue("check_library", i), nil
		case "absent":
			return q.issue("unlock", i), nil
		case "unlocked":
			return q.issue("receive", i), nil
		case "received":
			return q.issue("organize", i), nil
		case "organized", "ingested", "existing", "failed":
		default:
			return nil, fmt.Errorf("unknown task state %q", t.State)
		}
	}
	// Batch refresh happens only after every transfer/organize has settled.
	if !q.Refreshed && len(q.Tasks) > 0 {
		return q.issue("refresh_library", -1), nil
	}
	for n := 0; n < len(q.Tasks); n++ {
		i := (q.VerifyCursor + n) % len(q.Tasks)
		if q.Tasks[i].State == "organized" {
			return q.issue("verify_ingestion", i), nil
		}
	}
	return nil, nil
}
func (q *Queue) issue(kind string, i int) *Command {
	q.Revision++
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s/%d", q.RunID, q.Revision, kind, i)))
	c := Command{ID: hex.EncodeToString(h[:]), Kind: kind, Index: i, Revision: q.Revision}
	q.Pending = &c
	copy := c
	return &copy
}

// Resolve takes confirmed operation results, never just an accepted job ID.
// For receive/organize, adapters must poll until the native task is terminal.
// Verify success requires identity and actual media/episode coverage in Emby.
func (q *Queue) Resolve(id string, o Outcome) error {
	if q.Pending == nil || q.Pending.ID != id {
		return errors.New("stale operation result")
	}
	c := *q.Pending
	if o.Status == "unknown" {
		return nil
	}
	if o.Status != "success" && o.Status != "failure" && o.Status != "absent" {
		return errors.New("invalid outcome")
	}
	if c.Kind == "refresh_library" {
		if o.Status != "success" {
			return errors.New("refresh not confirmed; keep pending")
		}
		q.Refreshed = true
	} else {
		if c.Index < 0 || c.Index >= len(q.Tasks) {
			return errors.New("invalid task reference")
		}
		t := &q.Tasks[c.Index]
		if c.Kind == "verify_ingestion" && o.Status != "success" {
			// Rotate through all pending titles; a slow title must not hide others.
			q.VerifyCursor = (c.Index + 1) % len(q.Tasks)
			q.Pending = nil
			q.Revision++
			return nil
		}
		if o.Status == "failure" {
			t.State = "failed"
			t.Failure = o.Message
		} else {
			switch c.Kind {
			case "check_library":
				if o.Status == "absent" {
					t.State = "absent"
				} else {
					t.State = "existing"
				}
			case "unlock":
				if o.Status != "success" || o.Reference == "" {
					return errors.New("unlock requires a confirmed reference")
				}
				t.State = "unlocked"
				t.Reference = o.Reference
			case "receive":
				if o.Status != "success" || o.Reference == "" {
					return errors.New("receive requires a confirmed reference")
				}
				t.State = "received"
				t.Reference = o.Reference
			case "organize":
				if o.Status != "success" {
					return errors.New("organize not confirmed")
				}
				t.State = "organized"
			case "verify_ingestion":
				t.State = "ingested"
				q.VerifyCursor = (c.Index + 1) % len(q.Tasks)
			default:
				return errors.New("unknown command")
			}
		}
	}
	q.Pending = nil
	q.Revision++
	return nil
}

// ActiveMedia survives run boundaries to stop a six-hour scan from duplicating
// a title whose transfer finished but whose Emby ingestion is still pending.
func (q Queue) ActiveMedia() map[string]bool {
	r := map[string]bool{}
	for _, t := range q.Tasks {
		if t.State != "failed" && t.State != "existing" && t.State != "ingested" {
			r[mediaKey(t.Candidate.Media)] = true
		}
	}
	return r
}
