package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

var (
	wantClientContactFields = []string{
		"id", "dateAdded", "dateLastModified", "dateLastVisit",
		"name", "division", "source", "status", "owner", "type",
	}

	wantClientContactData = geckoboard.Data{
		{
			"id":              "1",
			"date_added":      stringPtr("2022-07-30T14:10:21Z"),
			"date_last_visit": stringPtr("2022-08-08T20:23:41Z"),
			"division":        "Div A",
			"owner":           stringPtr("Owner A"),
			"source":          "web",
			"status":          "Active",
			"type":            "Manager",
			"updated_at":      stringPtr("2022-07-30T15:00:21Z"),
		},
		{
			"id":              "2",
			"date_added":      stringPtr("2022-07-30T11:23:41Z"),
			"date_last_visit": stringPtr("2022-08-08T17:37:01Z"),
			"division":        "Div A",
			"owner":           stringPtr("Owner B"),
			"source":          "call",
			"status":          "Active",
			"type":            "Talent Aq.",
			"updated_at":      stringPtr("2022-07-30T12:13:41Z"),
		},
		{
			"id":              "3",
			"date_added":      stringPtr("2022-07-30T08:37:01Z"),
			"date_last_visit": stringPtr("2022-08-08T14:50:21Z"),
			"division":        "(not set)",
			"owner":           (*string)(nil),
			"source":          "(not set)",
			"status":          "Active",
			"type":            "(not set)",
			"updated_at":      stringPtr("2022-07-30T09:27:01Z"),
		},
	}

	testClientContacts = []bullhorn.ClientContact{
		{
			ID:               1,
			DateAdded:        1659190221000,
			DateLastModified: 1659193221000,
			DateListVisit:    1659990221000,

			CustomDate1:  0,
			CustomDate2:  1659194221000,
			CustomDate3:  1659195221000,
			CustomFloat1: 0,
			CustomFloat2: 2,
			CustomFloat3: 3,

			Name:     "Contact A",
			Division: "Div A",
			Source:   "web",
			Status:   "Active",
			Owner: bullhorn.Person{
				FirstName: "Owner",
				LastName:  "A",
			},
			Type: "Manager",
		},
		{
			ID:               2,
			DateAdded:        1659180221000,
			DateLastModified: 1659183221000,
			DateListVisit:    1659980221000,

			CustomDate1:  0,
			CustomDate2:  1659184221000,
			CustomDate3:  1659185221000,
			CustomFloat1: 0,
			CustomFloat2: 22,
			CustomFloat3: 23,

			Name:     "Contact B",
			Division: "Div A",
			Source:   "call",
			Status:   "Active",
			Owner: bullhorn.Person{
				FirstName: "Owner",
				LastName:  "B",
			},
			Type: "Talent Aq.",
		},
		{
			ID:               3,
			DateAdded:        1659170221000,
			DateListVisit:    1659970221000,
			DateLastModified: 1659173221000,

			CustomDate1:  0,
			CustomDate2:  1659174221000,
			CustomDate3:  1659175221000,
			CustomFloat1: 0,
			CustomFloat2: 32,
			CustomFloat3: 33,

			Name:     "",
			Division: "",
			Source:   "",
			Status:   "Active",
			Owner:    bullhorn.Person{},
			Type:     "",
		},
	}
)

func TestClientContact_String(t *testing.T) {
	jp := clientContactProcessor{}
	assert.Equal(t, jp.String(), "contact")
}

func TestClientContact_Schema(t *testing.T) {
	t.Run("builds schema with no custom fields", func(t *testing.T) {
		got := (&clientContactProcessor{}).Schema()
		want := &geckoboard.Dataset{
			Name: "bullhorn-contacts",
			Fields: map[string]geckoboard.Field{
				"id":              {Type: "string", Name: "ID"},
				"date_added":      {Type: "datetime", Name: "Date added", Optional: true},
				"date_last_visit": {Type: "datetime", Name: "Date last visit", Optional: true},
				"division":        {Type: "string", Name: "Division", Optional: true},
				"name":            {Type: "string", Name: "Name", Optional: true},
				"owner":           {Type: "string", Name: "Owner", Optional: true},
				"source":          {Type: "string", Name: "Source", Optional: true},
				"status":          {Type: "string", Name: "Status", Optional: true},
				"type":            {Type: "string", Name: "Type", Optional: true},
				"updated_at":      {Type: "datetime", Name: "Updated at", Optional: true},
			},
			UniqueBy: []string{"id"},
		}

		assert.DeepEqual(t, got, want)
	})

	t.Run("builds schema with custom fields", func(t *testing.T) {
		srv := &clientContactProcessor{
			customFields: []customField{
				{
					datasetField: "custom_date_1",
					fieldType:    "Date",
					displayName:  "Custom date 1",
				},
				{
					datasetField: "custom_float_1",
					fieldType:    "Float",
					displayName:  "Custom float 1",
				},
			},
		}
		got := srv.Schema()
		want := &geckoboard.Dataset{
			Name: "bullhorn-contacts",
			Fields: map[string]geckoboard.Field{
				"id":              {Type: "string", Name: "ID"},
				"date_added":      {Type: "datetime", Name: "Date added", Optional: true},
				"date_last_visit": {Type: "datetime", Name: "Date last visit", Optional: true},
				"division":        {Type: "string", Name: "Division", Optional: true},
				"name":            {Type: "string", Name: "Name", Optional: true},
				"owner":           {Type: "string", Name: "Owner", Optional: true},
				"source":          {Type: "string", Name: "Source", Optional: true},
				"status":          {Type: "string", Name: "Status", Optional: true},
				"type":            {Type: "string", Name: "Type", Optional: true},
				"updated_at":      {Type: "datetime", Name: "Updated at", Optional: true},
				"custom_date_1":   {Type: "datetime", Name: "Custom date 1", Optional: true},
				"custom_float_1":  {Type: "number", Name: "Custom float 1", Optional: true},
			},
			UniqueBy: []string{"id"},
		}

		assert.DeepEqual(t, got, want)
	})
}

func TestClientContact_QueryData(t *testing.T) {
	t.Run("returns all records successfully", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.ClientContactService = newClientContactService(t, testClientContacts)

		proc := clientContactProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantClientContactData)
	})

	t.Run("paginates until records are less than the count", func(t *testing.T) {
		bullhornRequests := 0

		bc := bullhorn.New("")
		bc.ClientContactService = mockClientContactService{
			searchFn: func(got bullhorn.SearchQuery) (*bullhorn.ClientContacts, error) {
				bullhornRequests += 1
				want := bullhorn.SearchQuery{
					Fields: wantClientContactFields,
					Where:  "isDeleted=false",
					Count:  2,
				}

				switch bullhornRequests {
				case 1:
					assert.DeepEqual(t, got, want)
					return &bullhorn.ClientContacts{
						Items: testClientContacts[:2],
					}, nil
				case 2:
					want.Start = 2 // Offset based on the count
					assert.DeepEqual(t, got, want)

					return &bullhorn.ClientContacts{
						Items: testClientContacts[2:],
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		proc := clientContactProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    2,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantClientContactData)
	})

	t.Run("paginates until total parsed records exceeds dataset max", func(t *testing.T) {
		bullhornRequests := 0

		bc := bullhorn.New("")
		bc.ClientContactService = mockClientContactService{
			searchFn: func(got bullhorn.SearchQuery) (*bullhorn.ClientContacts, error) {
				bullhornRequests += 1
				want := bullhorn.SearchQuery{
					Fields: wantClientContactFields,
					Where:  "isDeleted=false",
					Count:  1,
				}

				switch bullhornRequests {
				case 1:
					assert.DeepEqual(t, got, want)
					return &bullhorn.ClientContacts{
						Items: []bullhorn.ClientContact{testClientContacts[0]},
					}, nil
				case 2:
					return &bullhorn.ClientContacts{
						Items: []bullhorn.ClientContact{testClientContacts[1]},
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		proc := clientContactProcessor{
			client:            bc,
			maxDatasetRecords: 2,
			recordsPerPage:    1,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.DeepEqual(t, data, wantClientContactData[:2])
	})

	t.Run("returns only the max dataset records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.ClientContactService = newClientContactService(t, testClientContacts)

		proc := clientContactProcessor{
			client:            bc,
			maxDatasetRecords: 2,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 2)
		assert.DeepEqual(t, data, wantClientContactData[:2])
	})

	t.Run("returns empty data array when no contact records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.ClientContactService = newClientContactService(t, []bullhorn.ClientContact{})

		proc := clientContactProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 0)
		assert.DeepEqual(t, data, geckoboard.Data{})
	})

	t.Run("returns error when contact query fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.ClientContactService = mockClientContactService{
			searchFn: func(q bullhorn.SearchQuery) (*bullhorn.ClientContacts, error) {
				return nil, errors.New("query contacts failed")
			},
		}

		proc := clientContactProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		_, err := proc.QueryData(context.Background())
		assert.Error(t, err, "query contacts failed")
	})

	t.Run("custom fields", func(t *testing.T) {
		unsetEnv := func() {
			os.Unsetenv("CONTACT_CUSTOMFIELDS")
		}
		setEnv := func(val []string) {
			// Add space between to ensure we sanitize
			os.Setenv("CONTACT_CUSTOMFIELDS", strings.Join(val, " , "))
		}

		t.Run("queries extra custom fields and sets the data in the dataset", func(t *testing.T) {
			defer unsetEnv()

			fields := []string{"customDate2", "customFloat3"}
			bc := bullhorn.New("")
			bc.ClientContactService = mockClientContactService{
				searchFn: func(got bullhorn.SearchQuery) (*bullhorn.ClientContacts, error) {
					want := bullhorn.SearchQuery{
						Fields: append(wantClientContactFields, fields...),
						Where:  "isDeleted=false",
						Count:  200,
					}

					assert.DeepEqual(t, got, want)
					return &bullhorn.ClientContacts{
						Items: testClientContacts,
					}, nil
				},
			}

			proc := clientContactProcessor{client: bc, maxDatasetRecords: 50, recordsPerPage: 200}
			setEnv(fields)

			data, err := proc.QueryData(context.Background())
			assert.NilError(t, err)
			assert.DeepEqual(t, data, geckoboard.Data{
				{
					"custom_date_2":   stringPtr("2022-07-30T15:17:01Z"),
					"custom_float_3":  float64(3),
					"date_added":      stringPtr("2022-07-30T14:10:21Z"),
					"date_last_visit": stringPtr("2022-08-08T20:23:41Z"),
					"division":        "Div A",
					"id":              "1",
					"owner":           stringPtr("Owner A"),
					"source":          "web",
					"status":          "Active",
					"type":            "Manager",
					"updated_at":      stringPtr("2022-07-30T15:00:21Z"),
				},
				{
					"custom_date_2":   stringPtr("2022-07-30T12:30:21Z"),
					"custom_float_3":  float64(23),
					"date_added":      stringPtr("2022-07-30T11:23:41Z"),
					"date_last_visit": stringPtr("2022-08-08T17:37:01Z"),
					"division":        "Div A",
					"id":              "2",
					"owner":           stringPtr("Owner B"),
					"source":          "call",
					"status":          "Active",
					"type":            "Talent Aq.",
					"updated_at":      stringPtr("2022-07-30T12:13:41Z"),
				},
				{
					"custom_date_2":   stringPtr("2022-07-30T09:43:41Z"),
					"custom_float_3":  float64(33),
					"date_added":      stringPtr("2022-07-30T08:37:01Z"),
					"date_last_visit": stringPtr("2022-08-08T14:50:21Z"),
					"division":        "(not set)",
					"id":              "3",
					"owner":           (*string)(nil),
					"source":          "(not set)",
					"status":          "Active",
					"type":            "(not set)",
					"updated_at":      stringPtr("2022-07-30T09:27:01Z"),
				},
			})
		})

		t.Run("errors when invalid custom field", func(t *testing.T) {
			specs := []struct {
				name    string
				fields  []string
				wantErr string
			}{
				{
					name:   "invalid custom field",
					fields: []string{"customDate2", "customField2"},
					// Although customText0 isn't really supported for client contact if attempted it error appropriately
					// Test case for that below
					wantErr: `unknown contact field "customField2", only customDate0, customText0 and customFloat0 are valid`,
				},
				{
					name:    "customText1 is not supported",
					fields:  []string{"customDate2", "customText2"},
					wantErr: `custom field "customText2", is not supported for contact`,
				},
				{
					name:    "custom date field over range",
					fields:  []string{"customDate3", "customDate4"},
					wantErr: `contact field "customDate4", is out of range max field number is 3`,
				},
				{
					name:    "custom float field over range",
					fields:  []string{"customFloat3", "customFloat4"},
					wantErr: `contact field "customFloat4", is out of range max field number is 3`,
				},
			}

			for _, spec := range specs {
				t.Run(spec.name, func(t *testing.T) {
					defer unsetEnv()

					proc := clientContactProcessor{
						client:            bullhorn.New(""),
						maxDatasetRecords: 50,
						recordsPerPage:    200,
					}

					setEnv(spec.fields)
					_, gotErr := proc.QueryData(context.Background())
					assert.ErrorContains(t, gotErr, spec.wantErr)
				})
			}
		})
	})
}

type mockClientContactService struct {
	searchFn func(bullhorn.SearchQuery) (*bullhorn.ClientContacts, error)
}

func newClientContactService(t *testing.T, recs []bullhorn.ClientContact) mockClientContactService {
	return mockClientContactService{
		searchFn: func(got bullhorn.SearchQuery) (*bullhorn.ClientContacts, error) {
			want := bullhorn.SearchQuery{
				Fields: wantClientContactFields,
				Where:  "isDeleted=false",
				Count:  200,
			}

			assert.DeepEqual(t, got, want)
			return &bullhorn.ClientContacts{
				Items: recs,
			}, nil
		},
	}
}

func (m mockClientContactService) Search(_ context.Context, query bullhorn.SearchQuery) (*bullhorn.ClientContacts, error) {
	return m.searchFn(query)
}
