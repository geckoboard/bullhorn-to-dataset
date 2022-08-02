package bullhorn

import (
	"fmt"
	"strings"
	"time"
)

// SearchQuery defines the query params required
// for a successful entity search request
type SearchQuery struct {
	Fields []string
	Where  string
	Start  int
	Count  int
}

type EpochMilli uint64

func (e EpochMilli) Time() time.Time {
	return time.UnixMilli(int64(e)).UTC()
}

type NestedEntity struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
}

func (e EpochMilli) String() string {
	if e == 0 {
		return ""
	}

	return e.Time().Format(time.RFC3339)
}

type Owners struct {
	Items []Person `json:"data"`
}

type Person struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

func (p Person) FullName() *string {
	if p.FirstName == "" && p.LastName == "" {
		return nil
	}

	val := strings.TrimSpace(fmt.Sprintf("%s %s", p.FirstName, p.LastName))
	return &val
}
