package core

import "errors"

// Store must provide durable, atomic compare-and-swap, not a read/write emulation.
// DIAN Host Storage's ETag/If-Match is the intended implementation.
type Store interface {
	Load() (Queue, string, error)
	Save(Queue, string) (string, error)
}
type Adapter interface {
	Execute(Command, Queue) (Outcome, error)
	// Reconcile must query the existing job or use provider-guaranteed
	// idempotency with the original command ID. It must never create a new
	// paid unlock/transfer merely because the previous request timed out.
	Reconcile(Command, Queue) (Outcome, error)
}
type Runner struct {
	Store   Store
	Adapter Adapter
}

// Step performs one bounded action. No sleeps or parallel transfers. The host
// schedules scan every six hours and drives subsequent steps separately.
func (r Runner) Step() error {
	if r.Store == nil || r.Adapter == nil {
		return errors.New("adapter or storage unavailable")
	}
	q, etag, err := r.Store.Load()
	if err != nil {
		return err
	}
	recovering := q.Pending != nil
	var command Command
	if recovering {
		command = *q.Pending
	} else {
		c, e := q.Prepare()
		if e != nil {
			return e
		}
		if c == nil {
			return nil
		}
		command = *c
		etag, err = r.Store.Save(q, etag)
		if err != nil {
			return err
		}
	}
	var outcome Outcome
	if recovering {
		outcome, err = r.Adapter.Reconcile(command, q)
	} else {
		outcome, err = r.Adapter.Execute(command, q)
	}
	if err != nil {
		return err
	} // persisted Pending remains; do not advance
	if outcome.Status == "unknown" {
		return nil
	}
	if err = q.Resolve(command.ID, outcome); err != nil {
		return err
	}
	_, err = r.Store.Save(q, etag)
	return err
}
