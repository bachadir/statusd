package statusio

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"statusd/internal/catalog"
)

// MatchResult is the outcome of matching the catalogue to a Status.io page.
type MatchResult struct {
	Matched   map[string]Pair   // catalogue component ID -> pair
	Names     map[string]string // catalogue component ID -> "Status.io component / container"
	Unmatched []string          // catalogue items with no match
	Unused    []string          // Status.io pairs no catalogue component matched
}

var (
	parenRe   = regexp.MustCompile(`\([^)]*\)`)
	formerlRe = regexp.MustCompile(`(?i)\(formerly [^)]*\)`)
	wordRe    = regexp.MustCompile(`[a-z0-9]+`)
)

// norm makes names comparable: case, punctuation, spacing and plural "s" are
// ignored ("Real Time Feeds" = "Real-Time Feed").
func norm(s string) string {
	var b strings.Builder
	for _, w := range wordRe.FindAllString(strings.ToLower(s), -1) {
		if len(w) > 3 {
			w = strings.TrimSuffix(w, "s")
		}
		b.WriteString(w)
	}
	return b.String()
}

// names returns the forms a name is matched under: as is, without "(formerly
// …)", and without any bracket (legacy names are often added in brackets).
func names(s string) []string {
	out := []string{norm(s)}
	for _, v := range []string{formerlRe.ReplaceAllString(s, ""), parenRe.ReplaceAllString(s, "")} {
		if n := norm(v); n != "" && n != out[0] {
			out = append(out, n)
		}
	}
	return out
}

func sameName(a, b string) bool {
	for _, x := range names(a) {
		for _, y := range names(b) {
			if x == y {
				return true
			}
		}
	}
	return false
}

// legacy returns the normalised bracket of a name, e.g. "api" for
// "Public API (API)". Empty when there is none.
func legacy(s string) string {
	m := parenRe.FindString(formerlRe.ReplaceAllString(s, ""))
	return norm(m)
}

// Match pairs catalogue groups with Status.io components, then catalogue
// components with containers inside that component, both by name. Containers
// are shared between components in Status.io, so matching is always done
// within the group's component. A component with a single container takes it
// whatever its name (the website groups).
func Match(cat *catalog.Catalog, page []PageComponent) MatchResult {
	res := MatchResult{Matched: map[string]Pair{}, Names: map[string]string{}}
	used := map[Pair]bool{}
	usedComp := map[string]bool{}
	for _, g := range cat.Groups {
		var pc *PageComponent
		for i := range page {
			if !usedComp[page[i].ID] && sameName(g.Name, page[i].Name) {
				pc = &page[i]
				break
			}
		}
		if pc == nil {
			res.Unmatched = append(res.Unmatched, fmt.Sprintf("group %q (%s)", g.Name, g.Slug))
			continue
		}
		usedComp[pc.ID] = true
		for _, c := range g.Components {
			var hit *PageContainer
			if len(pc.Containers) == 1 && len(g.Components) == 1 {
				hit = &pc.Containers[0]
			} else {
				for i := range pc.Containers {
					pair := Pair{pc.ID, pc.Containers[i].ID}
					if !used[pair] && sameName(c.Name, pc.Containers[i].Name) {
						hit = &pc.Containers[i]
						break
					}
				}
				// Renamed, but the legacy name in brackets is the same: "(API)".
				if l := legacy(c.Name); hit == nil && l != "" {
					var only *PageContainer
					n := 0
					for i := range pc.Containers {
						if !used[Pair{pc.ID, pc.Containers[i].ID}] && legacy(pc.Containers[i].Name) == l {
							only, n = &pc.Containers[i], n+1
						}
					}
					if n == 1 {
						hit = only
					}
				}
			}
			if hit == nil {
				res.Unmatched = append(res.Unmatched, fmt.Sprintf("component %q (%s)", c.Name, c.ID))
				continue
			}
			pair := Pair{pc.ID, hit.ID}
			used[pair] = true
			res.Matched[c.ID] = pair
			res.Names[c.ID] = pc.Name + " / " + hit.Name
		}
	}
	for _, pc := range page {
		for _, ct := range pc.Containers {
			if !used[Pair{pc.ID, ct.ID}] {
				res.Unused = append(res.Unused, fmt.Sprintf("%s / %s (%s-%s)", pc.Name, ct.Name, pc.ID, ct.ID))
			}
		}
	}
	return res
}

// WriteMapping prints the statusio: lines to paste into catalog/*.yml.
func WriteMapping(w io.Writer, cat *catalog.Catalog, res MatchResult) {
	for _, g := range cat.Groups {
		first := true
		for _, c := range g.Components {
			p, ok := res.Matched[c.ID]
			if !ok {
				continue
			}
			if first {
				fmt.Fprintf(w, "# group %s (%s)\n", g.Slug, g.Name)
				first = false
			}
			fmt.Fprintf(w, "  - slug: %s  # %s\n    statusio: {component: %s, container: %s}  # %s\n",
				c.Slug, c.Name, p.Component, p.Container, res.Names[c.ID])
		}
	}
	if len(res.Unmatched) > 0 {
		fmt.Fprintln(w, "\n# not matched in the catalogue:")
		for _, u := range res.Unmatched {
			fmt.Fprintf(w, "#   %s\n", u)
		}
	}
	if len(res.Unused) > 0 {
		fmt.Fprintln(w, "\n# on the Status.io page but not matched:")
		for _, u := range res.Unused {
			fmt.Fprintf(w, "#   %s\n", u)
		}
	}
}
