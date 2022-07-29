package bullhorn

import "time"

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

type EntityWithName struct {
	Name string `json:"name"`
}

func (e EpochMilli) String() string {
	if e == 0 {
		return ""
	}

	return e.Time().Format(time.RFC3339)
}
