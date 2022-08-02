package bullhorn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"gotest.tools/v3/assert"
)

func TestClientContactService_Search(t *testing.T) {
	t.Run("returns client contacts", func(t *testing.T) {
		want := ClientContacts{
			Items: []ClientContact{
				{
					ID:               1,
					DateAdded:        EpochMilli(1659190221000),
					DateLastModified: EpochMilli(1659193221000),
					DateListVisit:    EpochMilli(1659990221000),

					CustomDate1:  EpochMilli(0),
					CustomDate2:  EpochMilli(1659194221000),
					CustomDate3:  EpochMilli(1659195221000),
					CustomFloat1: 0,
					CustomFloat2: 2,
					CustomFloat3: 3,

					Name:     "Contact A",
					Division: "divisonA",
					Owner: Person{
						FirstName: "Owner",
						LastName:  "AB",
					},
					Source: "web",
					Status: "Active",
					Type:   "Hiring Manager",
				},
				{
					ID:               2,
					DateAdded:        EpochMilli(1659190221000),
					DateLastModified: EpochMilli(1659193221000),
					DateListVisit:    EpochMilli(1659990221000),

					CustomDate1:  EpochMilli(0),
					CustomDate2:  EpochMilli(1659294221000),
					CustomDate3:  EpochMilli(1659295221000),
					CustomFloat1: 80,
					CustomFloat2: 82,
					CustomFloat3: 83,

					Name:     "Contact B",
					Division: "divisonB",
					Owner: Person{
						FirstName: "Owner",
						LastName:  "AB",
					},
					Source: "web",
					Status: "Closed",
					Type:   "Talent Acquisition",
				},
			},
		}

		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("BhRestToken"), "tok-456")
			assert.Equal(t, r.URL.Query().Get("fields"), "id,dateAdded,customDate1")
			assert.Equal(t, r.URL.Query().Get("where"), "isDeleted=false")
			assert.Equal(t, r.URL.Query().Get("start"), "0")
			assert.Equal(t, r.URL.Query().Get("count"), "200")

			json.NewEncoder(w).Encode(want)
		})

		defer server.Close()

		srv := &clientContactService{client: &Client{client: &http.Client{}, token: "tok-456"}, baseURL: server.URL}
		query := SearchQuery{
			Fields: []string{"id", "dateAdded", "customDate1"},
			Where:  "isDeleted=false",
			Start:  0,
			Count:  200,
		}

		got, err := srv.Search(context.Background(), query)
		assert.NilError(t, err)
		assert.DeepEqual(t, got, &want)
	})

	t.Run("returns error when request fails", func(t *testing.T) {
		srv := &clientContactService{client: New("")}
		_, err := srv.Search(context.Background(), SearchQuery{})
		assert.ErrorContains(t, err, "unsupported protocol scheme")
	})

	t.Run("returns error when request building fail", func(t *testing.T) {
		srv := &clientContactService{client: &Client{}, baseURL: string([]byte{0x7f})}
		_, err := srv.Search(context.Background(), SearchQuery{})
		assert.ErrorContains(t, err, "net/url: invalid control character in URL")
	})

	t.Run("returns error when non 200 response code", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "error invalid query")
		})
		defer server.Close()

		srv := &clientContactService{client: New(""), baseURL: server.URL}

		_, err := srv.Search(context.Background(), SearchQuery{})
		want := &Error{
			StatusCode:  http.StatusBadRequest,
			RequestPath: "/query/ClientContact",
			Message:     "error invalid query",
		}
		assert.DeepEqual(t, err, want)
	})
}
