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
	wantPlacementFields = []string{
		"id", "dateAdded", "dateBegin", "dateEnd", "dateLastModified", "employeeType", "employmentType",
		"fee", "jobOrder", "onboardingStatus", "referralFee", "referralFeeType", "status",
	}

	wantPlacementData = geckoboard.Data{
		{
			"date_added":        stringPtr("2022-07-30T14:10:21Z"),
			"date_begin":        stringPtr("2022-07-31T17:57:01Z"),
			"date_ended":        stringPtr("2022-08-08T20:23:41Z"),
			"employee_type":     "1",
			"employment_type":   "Contract",
			"fee":               float64(123),
			"id":                "1",
			"job_order":         "Job Title ABC (99)",
			"onboarding_status": "Completed",
			"referral_fee":      float64(25),
			"referral_fee_type": "percentage",
			"status":            "Active",
			"updated_at":        stringPtr("2022-07-30T15:00:21Z"),
		},
		{
			"date_added":        stringPtr("2022-07-30T11:23:41Z"),
			"date_begin":        stringPtr("2022-07-31T15:10:21Z"),
			"date_ended":        stringPtr("2022-08-08T17:37:01Z"),
			"employee_type":     "1",
			"employment_type":   "Contract",
			"fee":               float64(2123),
			"id":                "2",
			"job_order":         "Job Title CEF (299)",
			"onboarding_status": "Completed",
			"referral_fee":      float64(225),
			"referral_fee_type": "percentage",
			"status":            "Active",
			"updated_at":        stringPtr("2022-07-30T12:13:41Z"),
		},
		{
			"date_added":        stringPtr("2022-07-30T08:37:01Z"),
			"date_begin":        stringPtr("2022-07-31T12:23:41Z"),
			"date_ended":        stringPtr("2022-08-08T14:50:21Z"),
			"employee_type":     "Contractor",
			"employment_type":   "Contract",
			"fee":               float64(3123),
			"id":                "3",
			"job_order":         "Job Title GHI (399)",
			"onboarding_status": "Canceled",
			"referral_fee":      float64(0),
			"referral_fee_type": "",
			"status":            "Terminated",
			"updated_at":        stringPtr("2022-07-30T09:27:01Z"),
		},
	}

	testPlacements = []bullhorn.Placement{
		{
			ID:               1,
			DateAdded:        1659190221000,
			DateBegin:        1659290221000,
			DateEnd:          1659990221000,
			DateLastModified: 1659193221000,

			CustomDate1:  0,
			CustomDate2:  1659194221000,
			CustomDate3:  1659195221000,
			CustomDate4:  1659196221000,
			CustomText1:  "",
			CustomText2:  "text2",
			CustomText3:  "text3",
			CustomFloat1: 0,
			CustomFloat2: 2,
			CustomFloat3: 3,

			EmployeeType:   "1",
			EmploymentType: "Contract",
			Fee:            123,
			JobOrder: bullhorn.NestedEntity{
				ID:    99,
				Title: "Job Title ABC",
			},
			OnboardingStatus: "Completed",
			ReferralFee:      25,
			ReferralFeeType:  "percentage",
			Status:           "Active",
		},
		{
			ID:               2,
			DateAdded:        1659180221000,
			DateBegin:        1659280221000,
			DateEnd:          1659980221000,
			DateLastModified: 1659183221000,

			CustomDate1:  0,
			CustomDate2:  1659184221000,
			CustomDate3:  1659185221000,
			CustomDate4:  1659186221000,
			CustomText1:  "",
			CustomText2:  "text22",
			CustomText3:  "text23",
			CustomFloat1: 0,
			CustomFloat2: 22,
			CustomFloat3: 23,

			EmployeeType:   "1",
			EmploymentType: "Contract",
			Fee:            2123,
			JobOrder: bullhorn.NestedEntity{
				ID:    299,
				Title: "Job Title CEF",
			},
			OnboardingStatus: "Completed",
			ReferralFee:      225,
			ReferralFeeType:  "percentage",
			Status:           "Active",
		},
		{
			ID:               3,
			DateAdded:        1659170221000,
			DateBegin:        1659270221000,
			DateEnd:          1659970221000,
			DateLastModified: 1659173221000,

			CustomDate1:  0,
			CustomDate2:  1659174221000,
			CustomDate3:  1659175221000,
			CustomDate4:  1659176221000,
			CustomText1:  "",
			CustomText2:  "text32",
			CustomText3:  "text33",
			CustomFloat1: 0,
			CustomFloat2: 32,
			CustomFloat3: 33,

			EmployeeType:   "Contractor",
			EmploymentType: "Contract",
			Fee:            3123,
			JobOrder: bullhorn.NestedEntity{
				ID:    399,
				Title: "Job Title GHI",
			},
			OnboardingStatus: "Canceled",
			Status:           "Terminated",
		},
	}
)

func TestPlacement_String(t *testing.T) {
	jp := placementProcessor{}
	assert.Equal(t, jp.String(), "placement")
}

func TestPlacement_Schema(t *testing.T) {
	t.Run("builds schema with no custom fields", func(t *testing.T) {
		got := (&placementProcessor{}).Schema()
		want := &geckoboard.Dataset{
			Name: "bullhorn-placements",
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
				"date_begin": {
					Name: "Date Begin", Type: geckoboard.DatetimeType,
					Optional: true,
				},
				"date_ended": {
					Name: "Date ended", Type: geckoboard.DatetimeType,
					Optional: true,
				},
				"updated_at": {
					Name: "Updated at", Type: geckoboard.DatetimeType,
					Optional: true,
				},
				"employee_type": {
					Name:     "Employee type",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"employment_type": {
					Name:     "Employment type",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"fee": {
					Name:     "Fee %",
					Type:     geckoboard.PercentType,
					Optional: true,
				},
				"job_order": {
					Name:     "Job order",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"onboarding_status": {
					Name:     "Onboarding status",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"referral_fee": {
					Name:     "Referral fee",
					Type:     geckoboard.NumberType,
					Optional: true,
				},
				"referral_fee_type": {
					Name:     "Referral fee type",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"status": {
					Name:     "Status",
					Type:     geckoboard.StringType,
					Optional: true,
				},
			},
			UniqueBy: []string{"id"},
		}

		assert.DeepEqual(t, got, want)
	})

	t.Run("builds schema with custom fields", func(t *testing.T) {
		srv := &placementProcessor{
			customFields: []customField{
				{
					datasetField: "custom_text_1",
					fieldType:    "Text",
					displayName:  "Custom text 1",
				},
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
			Name: "bullhorn-placements",
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
				"date_begin": {
					Name: "Date Begin", Type: geckoboard.DatetimeType,
					Optional: true,
				},
				"date_ended": {
					Name: "Date ended", Type: geckoboard.DatetimeType,
					Optional: true,
				},
				"updated_at": {
					Name: "Updated at", Type: geckoboard.DatetimeType,
					Optional: true,
				},
				"employee_type": {
					Name:     "Employee type",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"employment_type": {
					Name:     "Employment type",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"fee": {
					Name:     "Fee %",
					Type:     geckoboard.PercentType,
					Optional: true,
				},
				"job_order": {
					Name:     "Job order",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"onboarding_status": {
					Name:     "Onboarding status",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"referral_fee": {
					Name:     "Referral fee",
					Type:     geckoboard.NumberType,
					Optional: true,
				},
				"referral_fee_type": {
					Name:     "Referral fee type",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"status": {
					Name:     "Status",
					Type:     geckoboard.StringType,
					Optional: true,
				},
				"custom_date_1": {
					Type:     "datetime",
					Name:     "Custom text 1",
					Optional: true,
				},
				"custom_float_1": {
					Type:     "number",
					Name:     "Custom float 1",
					Optional: true,
				},
				"custom_text_1": {
					Type:     "string",
					Name:     "Custom text 1",
					Optional: true,
				},
			},
			UniqueBy: []string{"id"},
		}

		assert.DeepEqual(t, got, want)
	})
}

func TestPlacement_QueryData(t *testing.T) {
	t.Run("returns all records successfully", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.PlacementService = newPlacementService(t, testPlacements)

		proc := placementProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantPlacementData)
	})

	t.Run("paginates until records are less than the count", func(t *testing.T) {
		bullhornRequests := 0

		bc := bullhorn.New("")
		bc.PlacementService = mockPlacementService{
			searchFn: func(got bullhorn.SearchQuery) (*bullhorn.Placements, error) {
				bullhornRequests += 1
				want := bullhorn.SearchQuery{
					Fields: wantPlacementFields,
					Where:  "id>0",
					Count:  2,
				}

				switch bullhornRequests {
				case 1:
					assert.DeepEqual(t, got, want)
					return &bullhorn.Placements{
						Items: testPlacements[:2],
					}, nil
				case 2:
					want.Start = 2 // Offset based on the count
					assert.DeepEqual(t, got, want)

					return &bullhorn.Placements{
						Items: testPlacements[2:],
					}, nil
				}

				return nil, errors.New("shouldn't have got here")
			},
		}

		proc := placementProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    2,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 3)
		assert.DeepEqual(t, data, wantPlacementData)
	})

	t.Run("returns only the max dataset records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.PlacementService = newPlacementService(t, testPlacements)

		proc := placementProcessor{
			client:            bc,
			maxDatasetRecords: 2,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 2)
		assert.DeepEqual(t, data, wantPlacementData[:2])
	})

	t.Run("returns empty data array when no placements records", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.PlacementService = newPlacementService(t, []bullhorn.Placement{})

		proc := placementProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		data, err := proc.QueryData(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(data), 0)
		assert.DeepEqual(t, data, geckoboard.Data{})
	})

	t.Run("returns error when placement query fails", func(t *testing.T) {
		bc := bullhorn.New("")
		bc.PlacementService = mockPlacementService{
			searchFn: func(q bullhorn.SearchQuery) (*bullhorn.Placements, error) {
				return nil, errors.New("query placements failed")
			},
		}

		proc := placementProcessor{
			client:            bc,
			maxDatasetRecords: 50,
			recordsPerPage:    200,
		}

		_, err := proc.QueryData(context.Background())
		assert.Error(t, err, "query placements failed")
	})

	t.Run("custom fields", func(t *testing.T) {
		unsetEnv := func() {
			os.Unsetenv("PLACEMENT_CUSTOMFIELDS")
		}
		setEnv := func(val []string) {
			// Add space between to ensure we sanitize
			os.Setenv("PLACEMENT_CUSTOMFIELDS", strings.Join(val, " , "))
		}

		t.Run("queries extra custom fields and sets the data in the dataset", func(t *testing.T) {
			defer unsetEnv()

			fields := []string{"customDate2", "customText2", "customFloat3"}
			bc := bullhorn.New("")
			bc.PlacementService = mockPlacementService{
				searchFn: func(got bullhorn.SearchQuery) (*bullhorn.Placements, error) {
					want := bullhorn.SearchQuery{
						Fields: append(wantPlacementFields, fields...),
						Where:  "id>0",
						Count:  200,
					}

					assert.DeepEqual(t, got, want)
					return &bullhorn.Placements{
						Items: testPlacements,
					}, nil
				},
			}

			proc := placementProcessor{client: bc, maxDatasetRecords: 50, recordsPerPage: 200}
			setEnv(fields)

			data, err := proc.QueryData(context.Background())
			assert.NilError(t, err)
			assert.DeepEqual(t, data, geckoboard.Data{
				{
					"custom_date_2":     stringPtr("2022-07-30T15:17:01Z"),
					"custom_float_3":    float64(3),
					"custom_text_2":     "text2",
					"date_added":        stringPtr("2022-07-30T14:10:21Z"),
					"date_begin":        stringPtr("2022-07-31T17:57:01Z"),
					"date_ended":        stringPtr("2022-08-08T20:23:41Z"),
					"employee_type":     "1",
					"employment_type":   "Contract",
					"fee":               float64(123),
					"id":                "1",
					"job_order":         "Job Title ABC (99)",
					"onboarding_status": "Completed",
					"referral_fee":      float64(25),
					"referral_fee_type": "percentage",
					"status":            "Active",
					"updated_at":        stringPtr("2022-07-30T15:00:21Z"),
				},
				{
					"custom_date_2":     stringPtr("2022-07-30T12:30:21Z"),
					"custom_float_3":    float64(23),
					"custom_text_2":     "text22",
					"date_added":        stringPtr("2022-07-30T11:23:41Z"),
					"date_begin":        stringPtr("2022-07-31T15:10:21Z"),
					"date_ended":        stringPtr("2022-08-08T17:37:01Z"),
					"employee_type":     "1",
					"employment_type":   "Contract",
					"fee":               float64(2123),
					"id":                "2",
					"job_order":         "Job Title CEF (299)",
					"onboarding_status": "Completed",
					"referral_fee":      float64(225),
					"referral_fee_type": "percentage",
					"status":            "Active",
					"updated_at":        stringPtr("2022-07-30T12:13:41Z"),
				},
				{
					"custom_date_2":     stringPtr("2022-07-30T09:43:41Z"),
					"custom_float_3":    float64(33),
					"custom_text_2":     "text32",
					"date_added":        stringPtr("2022-07-30T08:37:01Z"),
					"date_begin":        stringPtr("2022-07-31T12:23:41Z"),
					"date_ended":        stringPtr("2022-08-08T14:50:21Z"),
					"employee_type":     "Contractor",
					"employment_type":   "Contract",
					"fee":               float64(3123),
					"id":                "3",
					"job_order":         "Job Title GHI (399)",
					"onboarding_status": "Canceled",
					"referral_fee":      float64(0),
					"referral_fee_type": "",
					"status":            "Terminated",
					"updated_at":        stringPtr("2022-07-30T09:27:01Z"),
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
					name:    "invalid custom field name",
					fields:  []string{"customDate2", "customField2"},
					wantErr: `unknown placement field "customField2", only customDate0, customText0 and customFloat0 are valid`,
				},
				{
					name:    "custom date field over range",
					fields:  []string{"customDate13", "customDate14"},
					wantErr: `placement field "customDate14", is out of range max field number is 13`,
				},
				{
					name:    "custom date field under range",
					fields:  []string{"customDate0"},
					wantErr: `placement field "customDate0", is out of range min field number is 1`,
				},
				{
					name:    "custom text field over range",
					fields:  []string{"customText60", "customText61"},
					wantErr: `placement field "customText61", is out of range max field number is 60`,
				},
				{
					name:    "custom text field under range",
					fields:  []string{"customText0"},
					wantErr: `placement field "customText0", is out of range min field number is 1`,
				},
				{
					name:    "custom float field over range",
					fields:  []string{"customFloat23", "customFloat24"},
					wantErr: `placement field "customFloat24", is out of range max field number is 23`,
				},
				{
					name:    "custom float field under range",
					fields:  []string{"customFloat0"},
					wantErr: `placement field "customFloat0", is out of range min field number is 1`,
				},
			}

			for _, spec := range specs {
				t.Run(spec.name, func(t *testing.T) {
					defer unsetEnv()

					proc := placementProcessor{
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

type mockPlacementService struct {
	searchFn func(bullhorn.SearchQuery) (*bullhorn.Placements, error)
}

func newPlacementService(t *testing.T, recs []bullhorn.Placement) mockPlacementService {
	return mockPlacementService{
		searchFn: func(got bullhorn.SearchQuery) (*bullhorn.Placements, error) {
			want := bullhorn.SearchQuery{
				Fields: wantPlacementFields,
				Where:  "id>0",
				Count:  200,
			}

			assert.DeepEqual(t, got, want)
			return &bullhorn.Placements{
				Items: recs,
			}, nil
		},
	}
}

func (m mockPlacementService) Search(_ context.Context, query bullhorn.SearchQuery) (*bullhorn.Placements, error) {
	return m.searchFn(query)
}
