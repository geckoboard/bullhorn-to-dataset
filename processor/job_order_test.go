package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"errors"
	"testing"

	"gotest.tools/v3/assert"
)

var (
	wantJobOrderFields = []string{
		"id", "dateAdded", "dateClosed", "dateEnd", "status", "categories",
		"employmentType", "title", "owner", "clientCorporation", "isOpen",
	}

	wantJobOrdersData = geckoboard.Data{
		{
			"categories":         "Category A ; Category B",
			"client_corporation": "Los Pollos Hermanos",
			"date_added":         stringPtr("2022-05-22T10:19:47Z"),
			"date_closed":        (*string)(nil),
			"date_ended":         (*string)(nil),
			"employment_type":    "Contract",
			"id":                 "4345",
			"open":               "FALSE",
			"owner":              stringPtr("Gustavo Fring"),
			"status":             "Accepting Candidates",
			"title":              "Automation engineer",
		},
		{
			"categories":         "(not set)",
			"client_corporation": "Hamlin Hamlin McGill",
			"date_added":         stringPtr("2022-05-22T07:33:07Z"),
			"date_closed":        stringPtr("2022-05-22T10:19:47Z"),
			"date_ended":         stringPtr("2022-05-22T10:23:06Z"),
			"employment_type":    "Permanent",
			"id":                 "5555",
			"open":               "FALSE",
			"owner":              stringPtr("Kim Wexler"),
			"status":             "Closed",
			"title":              "CEO",
		},
		{
			"categories":         "Category C",
			"client_corporation": "JMM",
			"date_added":         stringPtr("2022-05-22T07:33:07Z"),
			"date_closed":        stringPtr("2022-05-22T10:19:47Z"),
			"date_ended":         stringPtr("2022-05-22T10:23:06Z"),
			"employment_type":    "Contract",
			"id":                 "3333",
			"open":               "FALSE",
			"owner":              stringPtr("Saul"),
			"status":             "Closed",
			"title":              "Support role",
		},
	}

	testJobOrders = []bullhorn.JobOrder{
		{
			ID:             4345,
			Title:          "Automation engineer",
			DateAdded:      1653214787000,
			Status:         "Accepting Candidates",
			EmploymentType: "Contract",
			Owner: bullhorn.Owner{
				FirstName: "Gustavo",
				LastName:  "Fring",
			},
			Client: bullhorn.NestedEntity{
				Name: "Los Pollos Hermanos",
			},
			Categories: bullhorn.Categories{
				Data: []bullhorn.NestedEntity{
					{Name: "Category B"},
					{Name: "Category A"},
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
			Owner: bullhorn.Owner{
				FirstName: "Kim",
				LastName:  "Wexler",
			},
			Client: bullhorn.NestedEntity{
				Name: "Hamlin Hamlin McGill",
			},
		},
		{
			ID:             3333,
			Title:          "Support role",
			DateAdded:      1653204787000,
			DateClosed:     1653214787000,
			DateEnd:        1653214986000,
			Status:         "Closed",
			EmploymentType: "Contract",
			Owner: bullhorn.Owner{
				FirstName: "Saul",
			},
			Client: bullhorn.NestedEntity{
				Name: "JMM",
			},
			Categories: bullhorn.Categories{
				Data: []bullhorn.NestedEntity{
					{Name: "Category C"},
				},
			},
		},
	}
)

func TestJobOrder_String(t *testing.T) {
	jp := jobOrderProcessor{}
	assert.Equal(t, jp.String(), "job order")
}

func TestJobOrder_Schema(t *testing.T) {
	got := jobOrderProcessor{}.Schema()
	want := &geckoboard.Dataset{
		Name: "bullhorn-joborders",
		Fields: map[string]geckoboard.Field{
			"id": {
				Name:     "ID",
				Type:     geckoboard.StringType,
				Optional: false,
			},
			"date_added": {
				Name: "Created at", Type: geckoboard.DatetimeType,
				Optional: true,
			},
			"date_closed": {
				Name: "Closed at", Type: geckoboard.DatetimeType,
				Optional: true,
			},
			"date_ended": {
				Name: "Ended at", Type: geckoboard.DatetimeType,
				Optional: true,
			},
			"title": {
				Name:     "Title",
				Type:     geckoboard.StringType,
				Optional: true,
			},
			"status": {
				Name:     "Status",
				Type:     geckoboard.StringType,
				Optional: true,
			},
			"categories": {
				Name:     "Categories",
				Type:     geckoboard.StringType,
				Optional: true,
			},
			"employment_type": {
				Name:     "Employment type",
				Type:     geckoboard.StringType,
				Optional: true,
			},
			"owner": {
				Name:     "Owner",
				Type:     geckoboard.StringType,
				Optional: true,
			},
			"client_corporation": {
				Name:     "Client corporation",
				Type:     geckoboard.StringType,
				Optional: true,
			},
			"open": {
				Name:     "Open",
				Type:     geckoboard.StringType,
				Optional: true,
			},
		},
		UniqueBy: []string{"id"},
	}

	assert.DeepEqual(t, got, want)
}

func TestJobOrder_QueryData(t *testing.T) {

	t.Run("returns all records successfully", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService(t, testJobOrders)

		proc := jobOrderProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			ordersPerPage:     200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantJobOrdersData)
	})

	t.Run("paginates until records are less than the count", func(t *testing.T) {
		bullhornRequests := 0

		bc := bullhorn.New("")
		bc.JobOrderService = mockJobOrderService{
			searchFn: func(got bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
				bullhornRequests += 1
				want := bullhorn.SearchQuery{
					Fields: wantJobOrderFields,
					Where:  "isDeleted=false",
					Count:  2,
				}

				switch bullhornRequests {
				case 1:
					assert.DeepEqual(t, got, want)
					return &bullhorn.JobOrders{
						Items: testJobOrders[:2],
					}, nil
				case 2:
					want.Start = 2 // Offset based on the count
					assert.DeepEqual(t, got, want)

					return &bullhorn.JobOrders{
						Items: testJobOrders[2:],
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		proc := jobOrderProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			ordersPerPage:     2,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantJobOrdersData)
	})

	t.Run("returns only the max dataset records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService(t, testJobOrders)

		proc := jobOrderProcessor{
			client:            bc,
			maxDatasetRecords: 2,
			ordersPerPage:     200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 2)
		assert.DeepEqual(t, data, wantJobOrdersData[:2])
	})

	t.Run("returns empty data array when no job order records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService(t, []bullhorn.JobOrder{})

		proc := jobOrderProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			ordersPerPage:     200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 0)
		assert.DeepEqual(t, data, geckoboard.Data{})
	})

	t.Run("returns error when job query fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = mockJobOrderService{
			searchFn: func(q bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
				return nil, errors.New("query job orders failed")
			},
		}

		proc := jobOrderProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			ordersPerPage:     200,
		}

		_, err := proc.QueryData(context.Background())
		assert.Error(t, err, "query job orders failed")
	})
}

// Mocks for the clients
type mockJobOrderService struct {
	searchFn func(bullhorn.SearchQuery) (*bullhorn.JobOrders, error)
}

func newJobOrderService(t *testing.T, recs []bullhorn.JobOrder) mockJobOrderService {
	return mockJobOrderService{
		searchFn: func(got bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
			want := bullhorn.SearchQuery{
				Fields: wantJobOrderFields,
				Where:  "isDeleted=false",
				Count:  200,
			}

			assert.DeepEqual(t, got, want)
			return &bullhorn.JobOrders{
				Items: recs,
			}, nil
		},
	}
}

func (m mockJobOrderService) Search(_ context.Context, query bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
	return m.searchFn(query)
}

func stringPtr(val string) *string {
	return &val
}
