package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"errors"
	"testing"

	"gotest.tools/v3/assert"
)

func TestProcessor_New(t *testing.T) {
	bc := &bullhorn.Client{}
	gc := &geckoboard.Client{}

	p := New(bc, gc)

	assert.Equal(t, p.bullhornClient, bc)
	assert.Equal(t, p.geckoboardClient, gc)
}

var testJobOrders = []bullhorn.JobOrder{
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
		Client: bullhorn.EntityWithName{
			Name: "Los Pollos Hermanos",
		},
		Categories: bullhorn.Categories{
			Data: []bullhorn.EntityWithName{
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
		Client: bullhorn.EntityWithName{
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
		Client: bullhorn.EntityWithName{
			Name: "JMM",
		},
		Categories: bullhorn.Categories{
			Data: []bullhorn.EntityWithName{
				{Name: "Category C"},
			},
		},
	},
}

func TestProcessor_Process(t *testing.T) {
	t.Run("processes all records successfully", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService(testJobOrders)

		gc := geckoboard.New("", "")
		dataSent := false

		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(got *geckoboard.Dataset) error {
				want := &geckoboard.Dataset{
					Name: "bullhorn-joborders",
					Fields: map[string]geckoboard.Field{
						"id":                 {Name: "ID", Type: geckoboard.StringType, Optional: false},
						"date_added":         {Name: "Created at", Type: geckoboard.DatetimeType, Optional: true},
						"date_closed":        {Name: "Closed at", Type: geckoboard.DatetimeType, Optional: true},
						"date_ended":         {Name: "Ended at", Type: geckoboard.DatetimeType, Optional: true},
						"title":              {Name: "Title", Type: geckoboard.StringType, Optional: true},
						"status":             {Name: "Status", Type: geckoboard.StringType, Optional: true},
						"categories":         {Name: "Categories", Type: geckoboard.StringType, Optional: true},
						"employment_type":    {Name: "Employment type", Type: geckoboard.StringType, Optional: true},
						"owner":              {Name: "Owner", Type: geckoboard.StringType, Optional: true},
						"client_corporation": {Name: "Client corporation", Type: geckoboard.StringType, Optional: true},
						"open":               {Name: "Open", Type: geckoboard.StringType, Optional: true},
					},
					UniqueBy: []string{"id"},
				}
				assert.DeepEqual(t, got, want)
				return nil
			},
			appendDataFn: func(_ *geckoboard.Dataset, data geckoboard.Data) error {
				dataSent = true

				assert.Equal(t, len(data), 3)
				assert.DeepEqual(t, data, geckoboard.Data{
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
				})
				return nil
			},
		}

		err := New(bc, gc).Process(context.Background())
		assert.NilError(t, err)
		assert.Assert(t, dataSent)
	})

	t.Run("paginates until records are less than the count", func(t *testing.T) {
		bullhornRequests := 0
		dataSent := false

		bc := bullhorn.New("")
		bc.JobOrderService = mockJobOrderService{
			searchFn: func(bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
				bullhornRequests += 1

				switch bullhornRequests {
				case 1:
					return &bullhorn.JobOrders{
						Items: testJobOrders[:2],
					}, nil
				case 2:
					return &bullhorn.JobOrders{
						Items: testJobOrders[2:],
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		gc := geckoboard.New("", "")

		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(got *geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(_ *geckoboard.Dataset, data geckoboard.Data) error {
				dataSent = true

				assert.Equal(t, len(data), 3)
				assert.DeepEqual(t, data, geckoboard.Data{
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
				})
				return nil
			},
		}

		p := New(bc, gc)
		p.bullhornRecordCount = 2

		err := p.Process(context.Background())
		assert.NilError(t, err)
		assert.Assert(t, dataSent)
	})

	t.Run("processes only max dataset records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService(testJobOrders)

		gc := geckoboard.New("", "")
		dataSent := false
		maxRecs := 2
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(*geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(_ *geckoboard.Dataset, data geckoboard.Data) error {
				dataSent = true
				assert.Equal(t, len(data), maxRecs)
				assert.DeepEqual(t, data, geckoboard.Data{
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
				})
				return nil
			},
		}

		p := New(bc, gc)
		p.maxDatasetRecords = maxRecs

		err := p.Process(context.Background())
		assert.NilError(t, err)
		assert.Assert(t, dataSent)
	})

	t.Run("processes when no job order records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService([]bullhorn.JobOrder{})

		gc := geckoboard.New("", "")
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(*geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(*geckoboard.Dataset, geckoboard.Data) error {
				return nil
			},
		}

		err := New(bc, gc).Process(context.Background())
		assert.NilError(t, err)
	})

	t.Run("returns error when job query fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = mockJobOrderService{
			searchFn: func(q bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
				return nil, errors.New("query job orders failed")
			},
		}

		err := New(bc, nil).Process(context.Background())
		assert.Error(t, err, "query job orders failed")
	})

	t.Run("returns error when geckoboard find or create dataset fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService([]bullhorn.JobOrder{{}})

		gc := geckoboard.New("", "")
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(*geckoboard.Dataset) error {
				return errors.New("failed to create dataset")
			},
		}

		err := New(bc, gc).Process(context.Background())
		assert.Error(t, err, "failed to create dataset")
	})

	t.Run("returns error when geckoboard find or create dataset fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobOrderService = newJobOrderService([]bullhorn.JobOrder{{}})

		gc := geckoboard.New("", "")
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(*geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(*geckoboard.Dataset, geckoboard.Data) error {
				return errors.New("push data error")
			},
		}

		err := New(bc, gc).Process(context.Background())
		assert.Error(t, err, "push data error")
	})
}

// Mocks for the clients

type mockDatasetService struct {
	findOrCreateFn func(*geckoboard.Dataset) error
	appendDataFn   func(*geckoboard.Dataset, geckoboard.Data) error
}

func (m mockDatasetService) FindOrCreate(_ context.Context, dataset *geckoboard.Dataset) error {
	return m.findOrCreateFn(dataset)
}

func (m mockDatasetService) AppendData(_ context.Context, dataset *geckoboard.Dataset, data geckoboard.Data) error {
	return m.appendDataFn(dataset, data)
}

type mockJobOrderService struct {
	searchFn func(bullhorn.SearchQuery) (*bullhorn.JobOrders, error)
}

func newJobOrderService(recs []bullhorn.JobOrder) mockJobOrderService {
	return mockJobOrderService{
		searchFn: func(bullhorn.SearchQuery) (*bullhorn.JobOrders, error) {
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
