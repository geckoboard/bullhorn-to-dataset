package bullhorn

import (
	"context"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type JobOrderService interface {
	Search(context.Context, SearchQuery) (*JobOrders, error)
}

type jobOrderService struct {
	baseURL string
	client  *Client
}

type JobOrders struct {
	Items []JobOrder `json:"data"`
}

type JobOrder struct {
	ID int

	DateAdded  EpochMilli `json:"dateAdded"`
	DateClosed EpochMilli `json:"dateClosed,omitempty"`
	DateEnd    EpochMilli `json:"dateEnd,omitempty"`

	Status         string       `json:"status"`
	Categories     Categories   `json:"categories"`
	EmploymentType string       `json:"employmentType"`
	Title          string       `json:"title"`
	Owner          Person       `json:"owner"`
	Client         NestedEntity `json:"clientCorporation"`
	IsOpen         bool         `json:"isOpen"`
}

type Categories struct {
	Data []NestedEntity `json:"data"`
}

func (c Categories) Join() string {
	cats := []string{}

	for _, c := range c.Data {
		cats = append(cats, c.Name)
	}

	// Sort so the categories are always consistent on the output
	// regardless of the order they are added
	sort.Strings(cats)
	return strings.Join(cats, " ; ")
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
