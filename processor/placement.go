package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"fmt"
	"math"
	"strconv"
)

var (
	placementCustomFieldRules = map[string]int{
		"Date":  13,
		"Text":  60,
		"Float": 23,
	}
)

type placementProcessor struct {
	client *bullhorn.Client

	maxDatasetRecords int
	recordsPerPage    int
	customFields      customFields
}

func (placementProcessor) String() string {
	return "placement"
}

func (p *placementProcessor) QueryData(ctx context.Context) (geckoboard.Data, error) {
	if err := p.customFields.fetchAndValidateCustomFields(p.String(), placementCustomFieldRules); err != nil {
		return nil, err
	}

	placements, err := p.queryPlacements(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Queried", len(placements), "placements")

	maxIndex := int(math.Min(float64(len(placements)), float64(p.maxDatasetRecords)))
	latestPlacements := placements[0:maxIndex]

	data := geckoboard.Data{}
	for _, r := range latestPlacements {
		entry := geckoboard.DataRow{
			"id":                strconv.Itoa(r.ID),
			"date_added":        valueOrNil(r.DateAdded.String()),
			"date_begin":        valueOrNil(r.DateBegin.String()),
			"date_ended":        valueOrNil(r.DateEnd.String()),
			"updated_at":        valueOrNil(r.DateLastModified.String()),
			"employee_type":     r.EmployeeType,
			"employment_type":   r.EmploymentType,
			"fee":               r.Fee,
			"job_order":         fmt.Sprintf("%s (%d)", r.JobOrder.Title, r.JobOrder.ID),
			"onboarding_status": r.OnboardingStatus,
			"referral_fee":      r.ReferralFee,
			"referral_fee_type": r.ReferralFeeType,
			"status":            r.Status,
		}

		p.customFields.extractCustomFieldData(r, entry)
		data = append(data, entry)
	}

	return data, nil
}

func (p *placementProcessor) Schema() *geckoboard.Dataset {
	datasetFields := map[string]geckoboard.Field{
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
	}

	p.customFields.extractCustomFieldsForSchema(datasetFields)

	return &geckoboard.Dataset{
		Name:     "bullhorn-placements",
		Fields:   datasetFields,
		UniqueBy: []string{"id"},
	}
}

func (p *placementProcessor) queryPlacements(ctx context.Context) ([]bullhorn.Placement, error) {
	var placements []bullhorn.Placement

	queryFields := []string{
		"id", "dateAdded", "dateBegin", "dateEnd", "dateLastModified",
		"employeeType", "employmentType", "fee", "jobOrder", "onboardingStatus",
		"referralFee", "referralFeeType", "status",
	}

	for _, f := range p.customFields {
		queryFields = append(queryFields, f.sanitized)
	}

	query := bullhorn.SearchQuery{
		Fields: queryFields,
		Where:  "id>0",
		Start:  0,
		Count:  p.recordsPerPage,
	}

	for {
		ps, err := p.client.PlacementService.Search(ctx, query)
		if err != nil {
			return nil, err
		}

		placements = append(placements, ps.Items...)

		if len(ps.Items) < query.Count {
			return placements, nil
		}

		query.Start = query.Count + query.Start
	}
}
