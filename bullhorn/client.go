package bullhorn

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	client *http.Client
	token  string

	AuthService          AuthService
	JobOrderService      JobOrderService
	PlacementService     PlacementService
	JobSubmissionService JobSubmissionService
}

func New(baseURL string) *Client {
	c := &Client{
		client: &http.Client{Timeout: 30 * time.Second},
	}

	c.AuthService = &authService{client: c, baseURL: baseURL}

	// These can't be used before having logged in because
	// they have a specific URL and token returned
	// from the login action so these are a nullService
	// implementing the JobOrderService which just return an error
	c.JobOrderService = nullJobOrderService{}
	c.PlacementService = nullPlacementService{}
	c.JobSubmissionService = nullJobSubmissionService{}

	return c
}

func (c *Client) setSession(s Session) {
	c.token = s.Value.Token

	c.JobOrderService = &jobOrderService{client: c, baseURL: s.Value.Endpoint}
	c.PlacementService = &placementService{client: c, baseURL: s.Value.Endpoint}
	c.JobSubmissionService = &jobSubmissionService{client: c, baseURL: s.Value.Endpoint}
}

func (c *Client) buildURL(baseURL, path string, params url.Values) string {
	return fmt.Sprintf("%s?%s", baseURL+path, params.Encode())
}

func (c *Client) buildGETRequest(url string) (*http.Request, error) {
	r, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	if c.token != "" {
		r.Header.Set("BhRestToken", c.token)
	}

	return r, nil
}

func (c *Client) doRequest(req *http.Request, resource interface{}) error {
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if err := c.checkResponse(resp); err != nil {
		return err
	}

	if resource != nil {
		d := json.NewDecoder(resp.Body)
		if err := d.Decode(&resource); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) checkResponse(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return &Error{
		StatusCode:  resp.StatusCode,
		RequestPath: resp.Request.URL.Path,
		Message:     string(b),
	}
}
