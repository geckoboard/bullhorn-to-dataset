package bullhorn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"gotest.tools/v3/assert"
)

func TestAuthService_Login(t *testing.T) {
	t.Run("returns nil and sets client session", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.URL.Query().Get("username"), "my-username")
			assert.Equal(t, r.URL.Query().Get("password"), "my-password")

			json.NewEncoder(w).Encode(User{
				Sessions: []Session{
					{
						Name: "rest",
						Value: SessionValue{
							Token:    "tok-123",
							Endpoint: "https://example.com",
						},
					},
					{
						Name: "extra",
						Value: SessionValue{
							Token:    "tok-666",
							Endpoint: "https://example.com/666",
						},
					},
				},
			})
		})

		defer server.Close()

		auth := &authService{client: New(server.URL), baseURL: server.URL}
		assert.Equal(t, auth.client.token, "")
		assert.Equal(t, fmt.Sprintf("%T", auth.client.JobOrderService), "bullhorn.nullSessionService")

		err := auth.Login(context.Background(), "my-username", "my-password")
		assert.NilError(t, err)

		assert.Equal(t, auth.client.token, "tok-123")
		assert.Equal(t, fmt.Sprintf("%T", auth.client.JobOrderService), "*bullhorn.jobOrderService")
	})

	t.Run("returns error when rest session not found", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(User{
				Sessions: []Session{
					{
						Name: "extra",
						Value: SessionValue{
							Token:    "tok-666",
							Endpoint: "https://example.com/666",
						},
					},
				},
			})
		})

		defer server.Close()

		auth := &authService{client: New(server.URL), baseURL: server.URL}

		err := auth.Login(context.Background(), "username", "password")
		assert.Error(t, err, errSessionNotFound.Error())
		assert.Equal(t, auth.client.token, "")
	})

	t.Run("returns error when request fails", func(t *testing.T) {
		auth := &authService{client: New("")}

		err := auth.Login(context.Background(), "username", "password")
		assert.ErrorContains(t, err, "unsupported protocol scheme")
	})

	t.Run("returns error when request building fail", func(t *testing.T) {
		auth := &authService{client: &Client{}, baseURL: string([]byte{0x7f})}

		err := auth.Login(context.Background(), "username", "password")
		assert.ErrorContains(t, err, "net/url: invalid control character in URL")
	})

	t.Run("returns error when non 200 response code", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, "error invalid token")
		})
		defer server.Close()

		auth := &authService{client: New(""), baseURL: server.URL}

		got := auth.Login(context.Background(), "username", "password")
		want := &Error{
			StatusCode:  http.StatusForbidden,
			RequestPath: "/universal-login/session/login",
			Message:     "error invalid token",
		}
		assert.DeepEqual(t, got, want)
	})

	t.Run("returns error when it fails parse json body", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "invalid json")
		})
		defer server.Close()

		auth := &authService{client: New(""), baseURL: server.URL}

		got := auth.Login(context.Background(), "username", "password")
		assert.ErrorType(t, got, &json.SyntaxError{})
	})
}

func buildMockServer(handlerFn func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(handlerFn))
}
