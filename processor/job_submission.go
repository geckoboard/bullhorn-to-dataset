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
	jobSubsCustomFieldRules = map[string]int{
		"Date":  5,
		"Float": 5,
	}
)

type jobSubmissionProcessor struct {
	client *bullhorn.Client

	maxDatasetRecords int
	recordsPerPage    int
	customFields      customFields
}

func (jobSubmissionProcessor) String() string {
	return "job submission"
}

func (p *jobSubmissionProcessor) QueryData(ctx context.Context) (geckoboard.Data, error) {
	if err := p.customFields.fetchAndValidateCustomFields(p.String(), jobSubsCustomFieldRules); err != nil {
		return nil, err
	}

	submissions, err := p.queryJobSubmissions(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Queried", len(submissions), "job submissions")

	maxIndex := int(math.Min(float64(len(submissions)), float64(p.maxDatasetRecords)))
	latestJobSubmissions := submissions[0:maxIndex]

	data := geckoboard.Data{}
	for _, js := range latestJobSubmissions {
		var ownerName string
		if len(js.Owners.Items) > 0 {
			ownerName = *js.Owners.Items[0].FullName()
		}

		entry := geckoboard.DataRow{
			"id":         strconv.Itoa(js.ID),
			"date_added": valueOrNil(js.DateAdded.String()),
			"end_date":   valueOrNil(js.EndDate.String()),
			"updated_at": valueOrNil(js.DateLastModified.String()),
			"job_order":  fmt.Sprintf("%s (%d)", js.JobOrder.Title, js.JobOrder.ID),
			"source":     valueOrNotSet(js.Source),
			"owner":      valueOrNil(ownerName),
			"candidate":  js.Candidate.FullName(),
			"status":     js.Status,
		}

		p.customFields.extractCustomFieldData(js, entry)
		data = append(data, entry)
	}

	return data, nil
}

func (p *jobSubmissionProcessor) Schema() *geckoboard.Dataset {
	datasetFields := map[string]geckoboard.Field{
		"id": {
			Name:     "ID",
			Type:     geckoboard.StringType,
			Optional: false,
		},
		"date_added": {
			Name: "Date Added", Type: geckoboard.DatetimeType,
			Optional: true,
		},
		"end_date": {
			Name: "End date", Type: geckoboard.DatetimeType,
			Optional: true,
		},
		"updated_at": {
			Name: "Updated at", Type: geckoboard.DatetimeType,
			Optional: true,
		},
		"source": {
			Name:     "Source",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"job_order": {
			Name:     "Job order",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"owner": {
			Name: "Owner", Type: geckoboard.StringType,
			Optional: true,
		},
		"candidate": {
			Name: "Candidate", Type: geckoboard.StringType,
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
		Name:     "bullhorn-job-submissions",
		Fields:   datasetFields,
		UniqueBy: []string{"id"},
	}
}

func (p *jobSubmissionProcessor) queryJobSubmissions(ctx context.Context) ([]bullhorn.JobSubmission, error) {
	var submissions []bullhorn.JobSubmission

	queryFields := []string{
		"id", "dateAdded", "endDate", "dateLastModified", "source",
		"status", "owners", "jobOrder", "candidate",
	}

	for _, f := range p.customFields {
		queryFields = append(queryFields, f.sanitized)
	}

	query := bullhorn.SearchQuery{
		Fields: queryFields,
		Where:  "isDeleted=false",
		Start:  0,
		Count:  p.recordsPerPage,
	}

	for {
		js, err := p.client.JobSubmissionService.Search(ctx, query)
		if err != nil {
			return nil, err
		}

		submissions = append(submissions, js.Items...)
		if len(js.Items) < query.Count || len(submissions) >= p.maxDatasetRecords {
			return submissions, nil
		}

		query.Start = query.Count + query.Start
	}
}
