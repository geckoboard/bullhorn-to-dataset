package bullhorn

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

type ClientContactService interface {
	Search(context.Context, SearchQuery) (*ClientContacts, error)
}

type clientContactService struct {
	baseURL string
	client  *Client
}

type ClientContacts struct {
	Items []ClientContact `json:"data"`
}

type ClientContact struct {
	ID int

	DateAdded        EpochMilli `json:"dateAdded"`
	DateLastModified EpochMilli `json:"dateLastModified"`
	DateListVisit    EpochMilli `json:"dateLastVisit"`

	Name     string `json:"name"`
	Division string `json:"division"`
	Source   string `json:"source"`
	Status   string `json:"status"`
	Owner    Person `json:"owner"`
	Type     string `json:"type"`

	CustomDate1  EpochMilli `json:"customDate1"`
	CustomDate2  EpochMilli `json:"customDate2"`
	CustomDate3  EpochMilli `json:"customDate3"`
	CustomFloat1 float64    `json:"customFloat1"`
	CustomFloat2 float64    `json:"customFloat2"`
	CustomFloat3 float64    `json:"customFloat3"`
}

func (c *clientContactService) Search(ctx context.Context, query SearchQuery) (*ClientContacts, error) {
	q := url.Values{}
	q.Add("fields", strings.Join(query.Fields, ","))
	q.Add("where", query.Where)
	q.Add("start", strconv.Itoa(query.Start))
	q.Add("count", strconv.Itoa(query.Count))
	q.Add("orderBy", "-id")

	req, err := c.client.buildGETRequest(c.client.buildURL(c.baseURL, "/query/ClientContact", q))
	if err != nil {
		return nil, err
	}

	contacts := &ClientContacts{}
	if err := c.client.doRequest(req.WithContext(ctx), contacts); err != nil {
		return nil, err
	}

	return contacts, nil
}
