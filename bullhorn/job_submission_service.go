package bullhorn

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

type JobSubmissionService interface {
	Search(context.Context, SearchQuery) (*JobSubmissions, error)
}

type jobSubmissionService struct {
	baseURL string
	client  *Client
}

type JobSubmissions struct {
	Items []JobSubmission `json:"data"`
}

type JobSubmission struct {
	ID int

	DateAdded        EpochMilli `json:"dateAdded"`
	EndDate          EpochMilli `json:"endDate,omitempty"`
	DateLastModified EpochMilli `json:"dateLastModified"`

	Source    string       `json:"source"`
	Status    string       `json:"status"`
	Owners    Owners       `json:"owners"`
	JobOrder  NestedEntity `json:"jobOrder"`
	Candidate Person       `json:"candidate"`

	CustomDate1 EpochMilli `json:"customDate1"`
	CustomDate2 EpochMilli `json:"customDate2"`
	CustomDate3 EpochMilli `json:"customDate3"`
	CustomDate4 EpochMilli `json:"customDate4"`
	CustomDate5 EpochMilli `json:"customDate5"`

	CustomFloat1 float64 `json:"customFloat1"`
	CustomFloat2 float64 `json:"customFloat2"`
	CustomFloat3 float64 `json:"customFloat3"`
	CustomFloat4 float64 `json:"customFloat4"`
	CustomFloat5 float64 `json:"customFloat5"`
}

func (j *jobSubmissionService) Search(ctx context.Context, query SearchQuery) (*JobSubmissions, error) {
	q := url.Values{}
	q.Add("fields", strings.Join(query.Fields, ","))
	q.Add("where", query.Where)
	q.Add("start", strconv.Itoa(query.Start))
	q.Add("count", strconv.Itoa(query.Count))
	q.Add("orderBy", "-id")

	req, err := j.client.buildGETRequest(j.client.buildURL(j.baseURL, "/query/JobSubmission", q))
	if err != nil {
		return nil, err
	}

	submissions := &JobSubmissions{}
	if err := j.client.doRequest(req.WithContext(ctx), submissions); err != nil {
		return nil, err
	}

	return submissions, nil
}
