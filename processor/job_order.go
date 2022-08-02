package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type jobOrderProcessor struct {
	client *bullhorn.Client

	maxDatasetRecords int
	ordersPerPage     int
}

func (j jobOrderProcessor) String() string {
	return "job order"
}

func (j jobOrderProcessor) QueryData(ctx context.Context) (geckoboard.Data, error) {
	jobOrders, err := j.queryJobOrders(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Queried", len(jobOrders), "job orders")

	maxIndex := int(math.Min(float64(len(jobOrders)), float64(j.maxDatasetRecords)))
	latestOrders := jobOrders[0:maxIndex]

	data := geckoboard.Data{}
	for _, o := range latestOrders {
		entry := geckoboard.DataRow{
			"id":                 strconv.Itoa(o.ID),
			"date_added":         valueOrNil(o.DateAdded.String()),
			"date_closed":        valueOrNil(o.DateClosed.String()),
			"date_ended":         valueOrNil(o.DateEnd.String()),
			"title":              o.Title,
			"status":             o.Status,
			"categories":         valueOrNotSet(o.Categories.Join()),
			"employment_type":    o.EmploymentType,
			"owner":              o.Owner.FullName(),
			"client_corporation": valueOrNotSet(o.Client.Name),
			"open":               strings.ToUpper(strconv.FormatBool(o.IsOpen)),
		}

		data = append(data, entry)
	}

	return data, nil
}

func (j jobOrderProcessor) Schema() *geckoboard.Dataset {
	return &geckoboard.Dataset{
		Name: "bullhorn-joborders",
		Fields: map[string]geckoboard.Field{
			"id": {
				Name:     "ID",
				Type:     geckoboard.StringType,
				Optional: false,
			},
			"date_added": {
				Name: "Date added", Type: geckoboard.DatetimeType,
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
}

func (j jobOrderProcessor) queryJobOrders(ctx context.Context) ([]bullhorn.JobOrder, error) {
	var jobOrders []bullhorn.JobOrder

	query := bullhorn.SearchQuery{
		Fields: []string{
			"id", "dateAdded", "dateClosed", "dateEnd", "status",
			"categories", "employmentType", "title", "owner",
			"clientCorporation", "isOpen",
		},
		Where: "isDeleted=false",
		Start: 0,
		Count: j.ordersPerPage,
	}

	for {
		jobs, err := j.client.JobOrderService.Search(ctx, query)
		if err != nil {
			return nil, err
		}

		jobOrders = append(jobOrders, jobs.Items...)

		if len(jobs.Items) < query.Count {
			return jobOrders, nil
		}

		query.Start = query.Count + query.Start
	}
}
