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
	wantJobSubmissionFields = []string{
		"id", "dateAdded", "endDate", "dateLastModified", "source",
		"status", "owners", "jobOrder", "candidate",
	}

	wantJobSubmissionData = geckoboard.Data{
		{
			"id":         "1",
			"candidate":  stringPtr("Candidate A"),
			"date_added": stringPtr("2022-07-30T14:10:21Z"),
			"end_date":   stringPtr("2022-08-08T20:23:41Z"),
			"job_order":  "Job Title ABC (99)",
			"owner":      stringPtr("Owner A"),
			"source":     "web",
			"status":     "Active",
			"updated_at": stringPtr("2022-07-30T15:00:21Z"),
		},
		{
			"id":         "2",
			"candidate":  stringPtr("Candidate B"),
			"date_added": stringPtr("2022-07-30T11:23:41Z"),
			"end_date":   stringPtr("2022-08-08T17:37:01Z"),
			"job_order":  "Job Title CEF (299)",
			"owner":      stringPtr("Owner B"),
			"source":     "lead",
			"status":     "Active",
			"updated_at": stringPtr("2022-07-30T12:13:41Z"),
		},
		{
			"id":         "3",
			"candidate":  (*string)(nil),
			"date_added": stringPtr("2022-07-30T08:37:01Z"),
			"end_date":   stringPtr("2022-08-08T14:50:21Z"),
			"job_order":  "Job Title GHI (399)",
			"owner":      (*string)(nil),
			"source":     "(not set)",
			"status":     "Terminated",
			"updated_at": stringPtr("2022-07-30T09:27:01Z"),
		},
	}

	testJobSubmissions = []bullhorn.JobSubmission{
		{
			ID:               1,
			DateAdded:        1659190221000,
			EndDate:          1659990221000,
			DateLastModified: 1659193221000,

			CustomDate1:  0,
			CustomDate2:  1659194221000,
			CustomDate3:  1659195221000,
			CustomDate4:  1659196221000,
			CustomFloat1: 0,
			CustomFloat2: 2,
			CustomFloat3: 3,

			Source: "web",
			JobOrder: bullhorn.NestedEntity{
				ID:    99,
				Title: "Job Title ABC",
			},
			Candidate: bullhorn.Person{
				FirstName: "Candidate",
				LastName:  "A",
			},
			Owners: bullhorn.Owners{
				Items: []bullhorn.Person{
					{
						FirstName: "Owner",
						LastName:  "A",
					},
				},
			},
			Status: "Active",
		},
		{
			ID:               2,
			DateAdded:        1659180221000,
			EndDate:          1659980221000,
			DateLastModified: 1659183221000,

			CustomDate1:  0,
			CustomDate2:  1659184221000,
			CustomDate3:  1659185221000,
			CustomDate4:  1659186221000,
			CustomFloat1: 0,
			CustomFloat2: 22,
			CustomFloat3: 23,

			Source: "lead",
			JobOrder: bullhorn.NestedEntity{
				ID:    299,
				Title: "Job Title CEF",
			},
			Candidate: bullhorn.Person{
				FirstName: "Candidate",
				LastName:  "B",
			},
			Owners: bullhorn.Owners{
				Items: []bullhorn.Person{
					{
						FirstName: "Owner",
						LastName:  "B",
					},
				},
			},
			Status: "Active",
		},
		{
			ID:               3,
			DateAdded:        1659170221000,
			EndDate:          1659970221000,
			DateLastModified: 1659173221000,

			CustomDate1:  0,
			CustomDate2:  1659174221000,
			CustomDate3:  1659175221000,
			CustomDate4:  1659176221000,
			CustomFloat1: 0,
			CustomFloat2: 32,
			CustomFloat3: 33,

			Source: "",
			JobOrder: bullhorn.NestedEntity{
				ID:    399,
				Title: "Job Title GHI",
			},
			Candidate: bullhorn.Person{},
			Owners:    bullhorn.Owners{},
			Status:    "Terminated",
		},
	}
)

func TestJobSubmission_String(t *testing.T) {
	jp := jobSubmissionProcessor{}
	assert.Equal(t, jp.String(), "job submission")
}

func TestJobSubmission_Schema(t *testing.T) {
	t.Run("builds schema with no custom fields", func(t *testing.T) {
		got := (&jobSubmissionProcessor{}).Schema()
		want := &geckoboard.Dataset{
			Name: "bullhorn-job-submissions",
			Fields: map[string]geckoboard.Field{
				"id":         {Name: "ID", Type: geckoboard.StringType, Optional: false},
				"date_added": {Name: "Date Added", Type: geckoboard.DatetimeType, Optional: true},
				"end_date":   {Name: "End date", Type: geckoboard.DatetimeType, Optional: true},
				"updated_at": {Name: "Updated at", Type: geckoboard.DatetimeType, Optional: true},
				"source":     {Name: "Source", Type: geckoboard.StringType, Optional: true},
				"job_order":  {Name: "Job order", Type: geckoboard.StringType, Optional: true},
				"owner":      {Name: "Owner", Type: geckoboard.StringType, Optional: true},
				"candidate":  {Name: "Candidate", Type: geckoboard.StringType, Optional: true},
				"status":     {Name: "Status", Type: geckoboard.StringType, Optional: true},
			},
			UniqueBy: []string{"id"},
		}

		assert.DeepEqual(t, got, want)
	})

	t.Run("builds schema with custom fields", func(t *testing.T) {
		srv := &jobSubmissionProcessor{
			customFields: []customField{
				{
					datasetField: "custom_date_1",
					fieldType:    "Date",
					displayName:  "Custom text 1",
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
			Name: "bullhorn-job-submissions",
			Fields: map[string]geckoboard.Field{
				"id":             {Name: "ID", Type: geckoboard.StringType, Optional: false},
				"date_added":     {Name: "Date Added", Type: geckoboard.DatetimeType, Optional: true},
				"end_date":       {Name: "End date", Type: geckoboard.DatetimeType, Optional: true},
				"updated_at":     {Name: "Updated at", Type: geckoboard.DatetimeType, Optional: true},
				"source":         {Name: "Source", Type: geckoboard.StringType, Optional: true},
				"job_order":      {Name: "Job order", Type: geckoboard.StringType, Optional: true},
				"owner":          {Name: "Owner", Type: geckoboard.StringType, Optional: true},
				"candidate":      {Name: "Candidate", Type: geckoboard.StringType, Optional: true},
				"status":         {Name: "Status", Type: geckoboard.StringType, Optional: true},
				"custom_date_1":  {Type: "datetime", Name: "Custom text 1", Optional: true},
				"custom_float_1": {Type: "number", Name: "Custom float 1", Optional: true},
			},
			UniqueBy: []string{"id"},
		}

		assert.DeepEqual(t, got, want)
	})
}

func TestJobSubmission_QueryData(t *testing.T) {
	t.Run("returns all records successfully", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobSubmissionService = newJobSubmissionService(t, testJobSubmissions)

		proc := jobSubmissionProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantJobSubmissionData)
	})

	t.Run("paginates until records are less than the count", func(t *testing.T) {
		bullhornRequests := 0

		bc := bullhorn.New("")
		bc.JobSubmissionService = mockJobSubmissionService{
			searchFn: func(got bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error) {
				bullhornRequests += 1
				want := bullhorn.SearchQuery{
					Fields: wantJobSubmissionFields,
					Where:  "isDeleted=false",
					Count:  2,
				}

				switch bullhornRequests {
				case 1:
					assert.DeepEqual(t, got, want)
					return &bullhorn.JobSubmissions{
						Items: testJobSubmissions[:2],
					}, nil
				case 2:
					want.Start = 2 // Offset based on the count
					assert.DeepEqual(t, got, want)

					return &bullhorn.JobSubmissions{
						Items: testJobSubmissions[2:],
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		proc := jobSubmissionProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    2,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantJobSubmissionData)
	})

	t.Run("paginates until total parsed records exceeds dataset max", func(t *testing.T) {
		bullhornRequests := 0

		bc := bullhorn.New("")
		bc.JobSubmissionService = mockJobSubmissionService{
			searchFn: func(got bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error) {
				bullhornRequests += 1
				want := bullhorn.SearchQuery{
					Fields: wantJobSubmissionFields,
					Where:  "isDeleted=false",
					Count:  1,
				}

				switch bullhornRequests {
				case 1:
					assert.DeepEqual(t, got, want)
					return &bullhorn.JobSubmissions{
						Items: []bullhorn.JobSubmission{testJobSubmissions[0]},
					}, nil
				case 2:
					return &bullhorn.JobSubmissions{
						Items: []bullhorn.JobSubmission{testJobSubmissions[1]},
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		proc := jobSubmissionProcessor{
			client:            bc,
			maxDatasetRecords: 2,
			recordsPerPage:    1,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.DeepEqual(t, data, wantJobSubmissionData[:2])
	})

	t.Run("returns only the max dataset records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobSubmissionService = newJobSubmissionService(t, testJobSubmissions)

		proc := jobSubmissionProcessor{
			client:            bc,
			maxDatasetRecords: 2,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 2)
		assert.DeepEqual(t, data, wantJobSubmissionData[:2])
	})

	t.Run("returns empty data array when no job submission records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobSubmissionService = newJobSubmissionService(t, []bullhorn.JobSubmission{})

		proc := jobSubmissionProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 0)
		assert.DeepEqual(t, data, geckoboard.Data{})
	})

	t.Run("returns error when job submission query fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.JobSubmissionService = mockJobSubmissionService{
			searchFn: func(q bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error) {
				return nil, errors.New("query job submission failed")
			},
		}

		proc := jobSubmissionProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		_, err := proc.QueryData(context.Background())
		assert.Error(t, err, "query job submission failed")
	})

	t.Run("custom fields", func(t *testing.T) {
		unsetEnv := func() {
			os.Unsetenv("JOBSUBMISSION_CUSTOMFIELDS")
		}
		setEnv := func(val []string) {
			// Add space between to ensure we sanitize
			os.Setenv("JOBSUBMISSION_CUSTOMFIELDS", strings.Join(val, " , "))
		}

		t.Run("queries extra custom fields and sets the data in the dataset", func(t *testing.T) {
			defer unsetEnv()

			fields := []string{"customDate2", "customFloat3"}
			bc := bullhorn.New("")
			bc.JobSubmissionService = mockJobSubmissionService{
				searchFn: func(got bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error) {
					want := bullhorn.SearchQuery{
						Fields: append(wantJobSubmissionFields, fields...),
						Where:  "isDeleted=false",
						Count:  200,
					}

					assert.DeepEqual(t, got, want)
					return &bullhorn.JobSubmissions{
						Items: testJobSubmissions,
					}, nil
				},
			}

			proc := jobSubmissionProcessor{client: bc, maxDatasetRecords: 50, recordsPerPage: 200}
			setEnv(fields)

			data, err := proc.QueryData(context.Background())
			assert.NilError(t, err)
			assert.DeepEqual(t, data, geckoboard.Data{
				{
					"id":             "1",
					"candidate":      stringPtr("Candidate A"),
					"custom_date_2":  stringPtr("2022-07-30T15:17:01Z"),
					"custom_float_3": float64(3),
					"date_added":     stringPtr("2022-07-30T14:10:21Z"),
					"end_date":       stringPtr("2022-08-08T20:23:41Z"),
					"job_order":      "Job Title ABC (99)",
					"owner":          stringPtr("Owner A"),
					"source":         "web",
					"status":         "Active",
					"updated_at":     stringPtr("2022-07-30T15:00:21Z"),
				},
				{
					"id":             "2",
					"candidate":      stringPtr("Candidate B"),
					"custom_date_2":  stringPtr("2022-07-30T12:30:21Z"),
					"custom_float_3": float64(23),
					"date_added":     stringPtr("2022-07-30T11:23:41Z"),
					"end_date":       stringPtr("2022-08-08T17:37:01Z"),
					"job_order":      "Job Title CEF (299)",
					"owner":          stringPtr("Owner B"),
					"source":         "lead",
					"status":         "Active",
					"updated_at":     stringPtr("2022-07-30T12:13:41Z"),
				},
				{
					"id":             "3",
					"candidate":      (*string)(nil),
					"custom_date_2":  stringPtr("2022-07-30T09:43:41Z"),
					"custom_float_3": float64(33),
					"date_added":     stringPtr("2022-07-30T08:37:01Z"),
					"end_date":       stringPtr("2022-08-08T14:50:21Z"),
					"job_order":      "Job Title GHI (399)",
					"owner":          (*string)(nil),
					"source":         "(not set)",
					"status":         "Terminated",
					"updated_at":     stringPtr("2022-07-30T09:27:01Z"),
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
					// Although customText0 isn't really supported for job submission if attempted it error appropriately
					// Test case for that below
					wantErr: `unknown job submission field "customField2", only customDate0, customText0 and customFloat0 are valid`,
				},
				{
					name:    "customText1 is not supported",
					fields:  []string{"customDate2", "customText2"},
					wantErr: `custom field "customText2", is not supported for job submission`,
				},
				{
					name:    "custom date field over range",
					fields:  []string{"customDate5", "customDate6"},
					wantErr: `job submission field "customDate6", is out of range max field number is 5`,
				},
				{
					name:    "custom float field over range",
					fields:  []string{"customFloat5", "customFloat6"},
					wantErr: `job submission field "customFloat6", is out of range max field number is 5`,
				},
			}

			for _, spec := range specs {
				t.Run(spec.name, func(t *testing.T) {
					defer unsetEnv()

					proc := jobSubmissionProcessor{
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

type mockJobSubmissionService struct {
	searchFn func(bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error)
}

func newJobSubmissionService(t *testing.T, recs []bullhorn.JobSubmission) mockJobSubmissionService {
	return mockJobSubmissionService{
		searchFn: func(got bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error) {
			want := bullhorn.SearchQuery{
				Fields: wantJobSubmissionFields,
				Where:  "isDeleted=false",
				Count:  200,
			}

			assert.DeepEqual(t, got, want)
			return &bullhorn.JobSubmissions{
				Items: recs,
			}, nil
		},
	}
}

func (m mockJobSubmissionService) Search(_ context.Context, query bullhorn.SearchQuery) (*bullhorn.JobSubmissions, error) {
	return m.searchFn(query)
}
