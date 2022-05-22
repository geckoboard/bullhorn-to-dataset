package geckoboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"gotest.tools/v3/assert"
)

func TestDatasetService_FindOrCreate(t *testing.T) {
	t.Run("successfully creates dataset", func(t *testing.T) {
		want := &Dataset{
			Name: "bullhorn-test",
			Fields: map[string]Field{
				"id": {
					Name:     "ID",
					Type:     StringType,
					Optional: false,
				},
				"created_at": {
					Name:     "Created at",
					Type:     DatetimeType,
					Optional: true,
				},
				"title": {
					Name:     "Index",
					Type:     NumberType,
					Optional: true,
				},
			},
			UniqueBy: []string{"id"},
		}

		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("Content-Type"), "application/json")
			assert.Equal(t, r.Header.Get("Authorization"), "Basic a2V5LTQ0NDo=")

			got := &Dataset{}
			if err := json.NewDecoder(r.Body).Decode(got); err != nil {
				t.Fatal(err)
			}

			assert.DeepEqual(t, got, want)
			w.WriteHeader(http.StatusOK)
		})
		defer server.Close()

		ds := newService(server.URL)
		assert.NilError(t, ds.FindOrCreate(context.Background(), want))
	})

	t.Run("returns error when marshaling body fails", func(t *testing.T) {
		ds := &datasetService{
			client: New("", "key-444"),
			jsonMarshalFn: func(interface{}) ([]byte, error) {
				return nil, errors.New("marshal error")
			},
		}
		err := ds.FindOrCreate(context.Background(), &Dataset{})
		assert.ErrorContains(t, err, "marshal error")
	})

	t.Run("returns error with invalid url", func(t *testing.T) {
		ds := newService(string([]byte{0x7f}))
		err := ds.FindOrCreate(context.Background(), &Dataset{})
		assert.ErrorContains(t, err, "invalid control character in URL")
	})

	t.Run("returns error when request fails", func(t *testing.T) {
		err := newService("").FindOrCreate(context.Background(), &Dataset{})
		assert.ErrorContains(t, err, "unsupported protocol scheme")
	})

	t.Run("returns error when response 500", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})
		defer server.Close()

		err := newService(server.URL).FindOrCreate(context.Background(), &Dataset{})
		assert.Error(t, err, errUnexpectedResponse.Error())
	})

	t.Run("returns geckoboard error when response 400", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"error":{"message": "invalid field type"}}`)
		})
		defer server.Close()

		err := newService(server.URL).FindOrCreate(context.Background(), &Dataset{})
		assert.DeepEqual(t, err, &Error{
			StatusCode: http.StatusBadRequest,
			Detail: Detail{
				Message: "invalid field type",
			},
		})
	})

	t.Run("returns err when it fail to parse error json", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{invalid json}`)
		})
		defer server.Close()

		err := newService(server.URL).FindOrCreate(context.Background(), &Dataset{})
		assert.ErrorType(t, err, &json.SyntaxError{})
	})
}

func TestDatasetService_AppendData(t *testing.T) {
	t.Run("makes a single data request", func(t *testing.T) {
		var requests int

		wantData := Data{
			{
				"id":         "1234",
				"title":      "My title",
				"created_at": "2022-05-10T11:12:13Z",
			},
		}

		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("Content-Type"), "application/json")
			assert.Equal(t, r.Header.Get("Authorization"), "Basic a2V5LTQ0NDo=")
			requests += 1

			got := &DataPayload{}
			if err := json.NewDecoder(r.Body).Decode(got); err != nil {
				t.Fatal(err)
			}

			assert.DeepEqual(t, got, &DataPayload{Data: wantData})
			w.WriteHeader(http.StatusNoContent)
		})
		defer server.Close()

		ds := newService(server.URL)
		ds.maxRecordsPerReq = 500

		err := ds.AppendData(context.Background(), &Dataset{Name: "test-dataset"}, wantData)
		assert.NilError(t, err)
		assert.Equal(t, requests, 1)
	})

	t.Run("makes a multiple data requests", func(t *testing.T) {
		var requests int
		wantData := Data{
			{"id": "3333", "title": "title one", "created_at": "2022-05-10T11:12:13Z"},
			{"id": "2222", "title": "title two", "created_at": "2022-02-10T11:12:13Z"},
			{"id": "1111", "title": "title three", "created_at": "2022-01-10T11:12:13Z"},
			{"id": "750", "title": "title four", "created_at": "2021-11-22T11:12:13Z"},
			{"id": "555", "title": "title five", "created_at": "2021-12-15T11:12:13Z"},
		}

		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("Content-Type"), "application/json")
			assert.Equal(t, r.Header.Get("Authorization"), "Basic a2V5LTQ0NDo=")
			requests += 1

			got := &DataPayload{}
			if err := json.NewDecoder(r.Body).Decode(got); err != nil {
				t.Fatal(err)
			}

			switch requests {
			case 1:
				assert.DeepEqual(t, got, &DataPayload{Data: wantData[0:2]})
			case 2:
				assert.DeepEqual(t, got, &DataPayload{Data: wantData[2:4]})
			case 3:
				assert.DeepEqual(t, got, &DataPayload{Data: wantData[4:]})
			}

			w.WriteHeader(http.StatusNoContent)
		})
		defer server.Close()

		ds := newService(server.URL)
		ds.maxRecordsPerReq = 2

		ctx := context.Background()
		err := ds.AppendData(ctx, &Dataset{Name: "test-dataset"}, wantData)
		assert.NilError(t, err)
		assert.Equal(t, requests, 3)
	})

	t.Run("returns error when request body marshal fails", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {})
		defer server.Close()

		ds := newService(server.URL)
		ds.jsonMarshalFn = func(interface{}) ([]byte, error) {
			return nil, errors.New("marshal error")
		}
		ds.maxRecordsPerReq = 2

		ctx := context.Background()
		err := ds.AppendData(ctx, &Dataset{Name: "test-dataset"}, Data{{}})
		assert.ErrorContains(t, err, "marshal error")
	})

	t.Run("returns error when building the request fails", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {})
		defer server.Close()

		ctx := context.Background()
		ds := newService(string([]byte{0x7f}))
		ds.maxRecordsPerReq = 1

		err := ds.AppendData(ctx, &Dataset{Name: "test-dataset"}, Data{{}, {}})
		assert.ErrorContains(t, err, "invalid control character in URL")
	})
}

func newService(url string) *datasetService {
	return &datasetService{
		client:           New(url, "key-444"),
		jsonMarshalFn:    json.Marshal,
		maxRecordsPerReq: 500,
	}
}

func buildMockServer(handlerFn func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(handlerFn))
}
