package bullhorn

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type JobOrderService interface {
	Search(context.Context, SearchQuery) (*JobOrders, error)
}

type jobOrderService struct {
	baseURL string
	client  *Client
}

// SearchQuery defines the query params required
// for a successfully job order search request
type SearchQuery struct {
	Fields []string
	Where  string
	Start  int
	Count  int
}

type JobOrders struct {
	Items []JobOrder `json:"data"`
}

type JobOrder struct {
	ID int

	DateAdded  EpochMilli `json:"dateAdded"`
	DateClosed EpochMilli `json:"dateClosed,omitempty"`
	DateEnd    EpochMilli `json:"dateEnd,omitempty"`

	Status         string         `json:"status"`
	Categories     Categories     `json:"categories"`
	EmploymentType string         `json:"employmentType"`
	Title          string         `json:"title"`
	Owner          Owner          `json:"owner"`
	Client         EntityWithName `json:"clientCorporation"`
	IsOpen         bool           `json:"isOpen"`
}

type EpochMilli uint64

func (e EpochMilli) Time() time.Time {
	return time.UnixMilli(int64(e)).UTC()
}

type Categories struct {
	Data []EntityWithName `json:"data"`
}

type EntityWithName struct {
	Name string `json:"name"`
}

type Owner struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

func (j *jobOrderService) Search(ctx context.Context, query SearchQuery) (*JobOrders, error) {
	q := url.Values{}
	q.Add("fields", strings.Join(query.Fields, ","))
	q.Add("where", query.Where)
	q.Add("start", strconv.Itoa(query.Start))
	q.Add("count", strconv.Itoa(query.Count))

	req, err := j.client.buildGETRequest(j.client.buildURL(j.baseURL, "/query/JobOrder", q))
	if err != nil {
		return nil, err
	}

	jobs := &JobOrders{}
	if err := j.client.doRequest(req.WithContext(ctx), jobs); err != nil {
		return nil, err
	}

	return jobs, nil
}
