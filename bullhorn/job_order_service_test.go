package bullhorn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestJobOrderService_Search(t *testing.T) {
	t.Run("returns job orders", func(t *testing.T) {
		want := JobOrders{
			Items: []JobOrder{
				{
					ID:             4345,
					Title:          "Automation engineer",
					DateAdded:      1653214787000,
					Status:         "Accepting Candidates",
					EmploymentType: "Contract",
					Owner: Owner{
						FirstName: "Sooo",
						LastName:  "Goodman",
					},
					Client: EntityWithName{
						Name: "GeckoOrg",
					},
					Categories: Categories{
						Data: []EntityWithName{
							{Name: "Category A"},
							{Name: "Category B"},
						},
					},
				},
				{
					ID:             5555,
					Title:          "CEO",
					DateAdded:      1653204787000,
					DateClosed:     1653214787000,
					DateEnd:        1653214986000,
					Status:         "Closed",
					EmploymentType: "Permanent",
					Owner: Owner{
						FirstName: "Sooo",
						LastName:  "Goodman",
					},
					Client: EntityWithName{
						Name: "GeckoOrg",
					},
				},
			},
		}

		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("BhRestToken"), "tok-456")
			assert.Equal(t, r.URL.Query().Get("fields"), "id,title,dateAdded")
			assert.Equal(t, r.URL.Query().Get("where"), "id>0")
			assert.Equal(t, r.URL.Query().Get("start"), "0")
			assert.Equal(t, r.URL.Query().Get("count"), "200")

			json.NewEncoder(w).Encode(want)
		})

		defer server.Close()

		job := &jobOrderService{client: &Client{client: &http.Client{}, token: "tok-456"}, baseURL: server.URL}
		query := SearchQuery{
			Fields: []string{"id", "title", "dateAdded"},
			Where:  "id>0",
			Start:  0,
			Count:  200,
		}

		got, err := job.Search(context.Background(), query)
		assert.NilError(t, err)
		assert.DeepEqual(t, got, &want)
	})

	t.Run("returns error when request fails", func(t *testing.T) {
		jos := &jobOrderService{client: New("")}

		_, err := jos.Search(context.Background(), SearchQuery{})
		assert.ErrorContains(t, err, "unsupported protocol scheme")
	})

	t.Run("returns error when request building fail", func(t *testing.T) {
		jos := &jobOrderService{client: &Client{}, baseURL: string([]byte{0x7f})}
		_, err := jos.Search(context.Background(), SearchQuery{})
		assert.ErrorContains(t, err, "net/url: invalid control character in URL")
	})

	t.Run("returns error when non 200 response code", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "error invalid query")
		})
		defer server.Close()

		jos := &jobOrderService{client: New(""), baseURL: server.URL}

		_, err := jos.Search(context.Background(), SearchQuery{})
		want := &Error{
			StatusCode:  http.StatusBadRequest,
			RequestPath: "/query/JobOrder",
			Message:     "error invalid query",
		}
		assert.DeepEqual(t, err, want)
	})

	t.Run("returns error when it fails parse json body", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "invalid json")
		})
		defer server.Close()

		jos := &jobOrderService{client: New(""), baseURL: server.URL}

		_, err := jos.Search(context.Background(), SearchQuery{})
		assert.ErrorType(t, err, &json.SyntaxError{})
	})
}

func TestEpochMilli_Time(t *testing.T) {
	t.Run("returns parsed time value", func(t *testing.T) {
		unix := EpochMilli(1653215692000)
		assert.DeepEqual(t, unix.Time(), time.Date(2022, 5, 22, 10, 34, 52, 0, time.UTC))
	})
}
