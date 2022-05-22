package bullhorn

import (
	"context"
	"errors"
	"net/url"
)

var errSessionNotFound = errors.New("rest session not found")

type AuthService interface {
	Login(_ context.Context, username, password string) error
}

type authService struct {
	baseURL string
	client  *Client
}

type Session struct {
	Name  string       `json:"name"`
	Value SessionValue `json:"value"`
}

type SessionValue struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

type Sessions []Session

type User struct {
	Sessions Sessions `json:"sessions"`
}

func (sessions Sessions) FindByName(name string) *Session {
	for _, session := range sessions {
		if session.Name == name {
			return &session
		}
	}

	return nil
}

func (a *authService) Login(ctx context.Context, username, password string) error {
	q := url.Values{}
	q.Add("username", username)
	q.Add("password", password)

	url := a.client.buildURL(a.baseURL, "/universal-login/session/login", q)
	req, err := a.client.buildGETRequest(url)
	if err != nil {
		return err
	}

	user := &User{}
	if err := a.client.doRequest(req.WithContext(ctx), user); err != nil {
		return err
	}

	session := user.Sessions.FindByName("rest")
	if session == nil {
		return errSessionNotFound
	}

	a.client.setSession(*session)
	return nil
}
