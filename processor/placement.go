package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	customFieldRegexp   = regexp.MustCompile(`^(custom)(Date|Text|Float)(\d{1,2})$`)
	customFieldMaxRange = map[string]int{
		"Date":  13,
		"Text":  60,
		"Float": 23,
	}
)

type placementProcessor struct {
	client *bullhorn.Client

	maxDatasetRecords int
	recordsPerPage    int
	customFields      []customField
}

func (placementProcessor) String() string {
	return "placement"
}

func (p *placementProcessor) QueryData(ctx context.Context) (geckoboard.Data, error) {
	if err := p.fetchAndValidateCustomFields(); err != nil {
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

		p.extractCustomFieldData(r, entry)
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

	p.extractCustomFieldsForSchema(datasetFields)

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

type customFieldError struct {
	field      string
	fieldValid bool

	underRange bool
	maxRange   int
}

func (e customFieldError) Error() string {
	if !e.fieldValid {
		return fmt.Sprintf("unknown placement field %q, only customDate0, customText0 and customFloat0 are valid", e.field)
	}

	if e.underRange {
		return fmt.Sprintf("custom placement field %q, is out of range min field number is 1", e.field)
	}

	return fmt.Sprintf("custom placement field %q, is out of range max field number is %d", e.field, e.maxRange)
}

type customField struct {
	sanitized    string
	datasetField string
	structField  string
	fieldType    string
	displayName  string
}

func (p *placementProcessor) fetchAndValidateCustomFields() error {
	env := os.Getenv("PLACEMENT_CUSTOMFIELDS")
	if env == "" {
		return nil
	}

	rawFields := strings.Split(env, ",")
	fields := []customField{}

	for _, f := range rawFields {
		field := strings.TrimSpace(f)

		if !customFieldRegexp.MatchString(field) {
			return customFieldError{field: field}
		}

		parts := customFieldRegexp.FindStringSubmatch(field)
		num, _ := strconv.Atoi(parts[3])

		err := customFieldError{field: field, fieldValid: true}
		if num <= 0 {
			err.underRange = true
			return err
		}

		maxRange := customFieldMaxRange[parts[2]]
		if num > maxRange {
			err.maxRange = maxRange
			return err
		}

		fields = append(fields, customField{
			sanitized:    field,
			datasetField: strings.ToLower(strings.Join(parts[1:], "_")),
			structField:  strings.Title(field),
			displayName:  strings.Join(parts[1:], " "),
			fieldType:    parts[2],
		})
	}

	p.customFields = fields
	return nil
}

func (p *placementProcessor) extractCustomFieldData(placement bullhorn.Placement, row geckoboard.DataRow) {
	ref := reflect.Indirect(reflect.ValueOf(placement))

	for _, f := range p.customFields {
		val := ref.FieldByName(f.structField)

		switch f.fieldType {
		case "Text":
			row[f.datasetField] = val.String()
		case "Float":
			row[f.datasetField] = val.Float()
		case "Date":
			epoch, _ := val.Interface().(bullhorn.EpochMilli)
			row[f.datasetField] = valueOrNil(epoch.String())
		}
	}
}

func (p *placementProcessor) extractCustomFieldsForSchema(fields map[string]geckoboard.Field) {
	for _, f := range p.customFields {
		switch f.fieldType {
		case "Text":
			fields[f.datasetField] = geckoboard.Field{
				Name:     f.displayName,
				Type:     geckoboard.StringType,
				Optional: true,
			}
		case "Float":
			fields[f.datasetField] = geckoboard.Field{
				Name:     f.displayName,
				Type:     geckoboard.NumberType,
				Optional: true,
			}
		case "Date":
			fields[f.datasetField] = geckoboard.Field{
				Name:     f.displayName,
				Type:     geckoboard.DatetimeType,
				Optional: true,
			}
		}
	}
}
