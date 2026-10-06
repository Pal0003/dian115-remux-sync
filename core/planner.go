// Package core contains network-independent planning and durable queue transitions.
// Adapters must prove complete snapshots before calling Plan.
package core

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type Media struct {
	Type  string `json:"type"` // movie or tv
	TMDB  int64  `json:"tmdb_id"`
	Title string `json:"title"`
	Year  int    `json:"year"`
}

type Episode struct {
	Season int `json:"season"`
	Number int `json:"number"`
}

type Candidate struct {
	Media         Media     `json:"media"`
	ResourceID    int64     `json:"resource_id"`
	ShareID       int64     `json:"share_id"`
	Kind          string    `json:"kind"`
	Remux         bool      `json:"remux"`
	Bytes         int64     `json:"bytes"`
	Episodes      []Episode `json:"episodes,omitempty"`
	CoverageKnown bool      `json:"coverage_known"`
	Points        int64     `json:"points"`
}

type Library struct {
	ID       string  `json:"id"`
	Complete bool    `json:"complete"`
	Items    []Media `json:"items"`
}

type Snapshot struct {
	InFlight        []Media     `json:"in_flight"`
	CatalogComplete bool        `json:"catalog_complete"`
	Candidates      []Candidate `json:"candidates"`
	// IDs come from the configured Emby instance's entire movie/TV library list.
	RequiredLibraries []string  `json:"required_libraries"`
	Libraries         []Library `json:"libraries"`
}

type Decision struct {
	Candidate Candidate `json:"candidate"`
	Reason    string    `json:"reason"`
}
type PlanResult struct {
	Selected []Candidate `json:"selected"`
	Skipped  []Decision  `json:"skipped"`
	Held     []Decision  `json:"held"`
}

func mediaKey(m Media) string { return fmt.Sprintf("%s:%d", m.Type, m.TMDB) }
func titleKey(m Media) string {
	if m.Year <= 0 || strings.TrimSpace(m.Title) == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d:%s", m.Type, m.Year, strings.Join(strings.Fields(strings.ToLower(m.Title)), " "))
}
func validMedia(m Media) bool { return m.Type == "movie" || m.Type == "tv" }

func episodeCount(c Candidate) int {
	seen := map[Episode]bool{}
	for _, e := range c.Episodes {
		if e.Season >= 0 && e.Number > 0 {
			seen[e] = true
		}
	}
	return len(seen)
}

func better(a, b Candidate) bool {
	if a.Media.Type == "tv" && episodeCount(a) != episodeCount(b) {
		return episodeCount(a) > episodeCount(b)
	}
	if a.Bytes != b.Bytes {
		return a.Bytes > b.Bytes
	}
	return a.ShareID < b.ShareID // stable across pagination and repeated scans
}

// Plan requires a full catalog scan; page 1 cannot select a smaller duplicate
// before a more complete share appears on page 2. Existing TV titles are skipped
// even when incomplete: this plugin collects absent titles, not gap filling.
func Plan(s Snapshot) (PlanResult, error) {
	r := PlanResult{}
	if !s.CatalogComplete {
		return r, errors.New("REMUX catalog snapshot is incomplete")
	}
	if len(s.RequiredLibraries) == 0 {
		return r, errors.New("no verified Emby libraries")
	}
	libs := map[string]Library{}
	for _, l := range s.Libraries {
		if _, ok := libs[l.ID]; ok {
			return r, errors.New("duplicate library snapshot")
		}
		libs[l.ID] = l
	}
	owned := map[string]bool{}
	titles := map[string][]Media{}
	for _, id := range s.RequiredLibraries {
		l, ok := libs[id]
		if !ok || !l.Complete {
			return r, fmt.Errorf("library %s is incomplete", id)
		}
		for _, m := range l.Items {
			if !validMedia(m) {
				continue
			}
			if m.TMDB > 0 {
				owned[mediaKey(m)] = true
			}
			if k := titleKey(m); k != "" {
				titles[k] = append(titles[k], m)
			}
		}
	}
	groups := map[string][]Candidate{}
	active := map[string]bool{}
	for _, m := range s.InFlight {
		if validMedia(m) && m.TMDB > 0 {
			active[mediaKey(m)] = true
		}
	}
	for _, c := range s.Candidates {
		if c.Kind != "115" || !c.Remux {
			r.Skipped = append(r.Skipped, Decision{c, "not a REMUX 115 share"})
			continue
		}
		if !validMedia(c.Media) || c.Media.TMDB <= 0 {
			r.Held = append(r.Held, Decision{c, "unverified media identity, size, price or share identity"})
			continue
		}
		k := mediaKey(c.Media)
		if active[k] {
			r.Skipped = append(r.Skipped, Decision{c, "already queued or awaiting Emby ingestion"})
			continue
		}
		if owned[k] {
			r.Skipped = append(r.Skipped, Decision{c, "already in an Emby library"})
			continue
		}
		ambiguous := false
		for _, m := range titles[titleKey(c.Media)] {
			if m.TMDB <= 0 {
				ambiguous = true
			}
		}
		if ambiguous {
			r.Held = append(r.Held, Decision{c, "same title/year in Emby has no TMDB ID"})
			continue
		}
		groups[k] = append(groups[k], c)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		cs := groups[k]
		uncertain := false
		for _, c := range cs {
			if c.ShareID <= 0 || c.ResourceID <= 0 || c.Bytes <= 0 || c.Points < 0 {
				uncertain = true
			}
			if c.Media.Type == "tv" {
				if !c.CoverageKnown || episodeCount(c) == 0 {
					uncertain = true
				}
				for _, e := range c.Episodes {
					if e.Season < 0 || e.Number <= 0 {
						uncertain = true
					}
				}
			}
		}
		// Unknown coverage could conceal the most complete option. Do not pick
		// a known but inferior share simply because one competitor lacks metadata.
		if uncertain {
			for _, c := range cs {
				r.Held = append(r.Held, Decision{c, "share metadata or episode coverage requires verification"})
			}
			continue
		}
		sort.SliceStable(cs, func(i, j int) bool { return better(cs[i], cs[j]) })
		r.Selected = append(r.Selected, cs[0])
		for _, c := range cs[1:] {
			r.Skipped = append(r.Skipped, Decision{c, "less complete or smaller duplicate"})
		}
	}
	return r, nil
}
