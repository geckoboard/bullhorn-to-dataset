package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Processor contains clients to push and pull data
type Processor struct {
	bullhornClient   *bullhorn.Client
	geckoboardClient *geckoboard.Client

	maxDatasetRecords   int
	bullhornRecordCount int
}

func New(bc *bullhorn.Client, gc *geckoboard.Client) Processor {
	return Processor{
		bullhornClient:      bc,
		geckoboardClient:    gc,
		maxDatasetRecords:   5000,
		bullhornRecordCount: 200,
	}
}

// Process handles querying the data from Bullhorn
// processing it and pushing to Geckoboard
func (p Processor) Process(ctx context.Context) error {
	jobOrders, err := p.queryJobOrders(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Queried", len(jobOrders), "job orders")

	maxIndex := int(math.Min(float64(len(jobOrders)), float64(p.maxDatasetRecords)))
	latestOrders := jobOrders[0:maxIndex]

	dataset := p.buildDatasetSchema()
	if err := p.geckoboardClient.DatasetService.FindOrCreate(ctx, dataset); err != nil {
		return err
	}

	data := geckoboard.Data{}
	for _, o := range latestOrders {
		entry := geckoboard.DataRow{
			"id":                 strconv.Itoa(o.ID),
			"date_added":         p.timeValueOrNil(o.DateAdded),
			"date_closed":        p.timeValueOrNil(o.DateClosed),
			"date_ended":         p.timeValueOrNil(o.DateEnd),
			"title":              o.Title,
			"status":             o.Status,
			"categories":         p.valueOrNotSet(o.Categories.Join()),
			"employment_type":    o.EmploymentType,
			"owner":              o.Owner.FullName(),
			"client_corporation": p.valueOrNotSet(o.Client.Name),
			"open":               strings.ToUpper(strconv.FormatBool(o.IsOpen)),
		}

		data = append(data, entry)
	}

	fmt.Println("Pushing", len(data), "records to geckoboard")
	return p.geckoboardClient.DatasetService.AppendData(ctx, dataset, data)
}

func (p Processor) timeValueOrNil(t bullhorn.EpochMilli) *string {
	if t == 0 {
		return nil
	}

	val := t.Time().Format(time.RFC3339)
	return &val
}

func (p Processor) valueOrNotSet(v string) string {
	if v == "" {
		return "(not set)"
	}

	return v

}

func (p Processor) queryJobOrders(ctx context.Context) ([]bullhorn.JobOrder, error) {
	var jobOrders []bullhorn.JobOrder

	query := bullhorn.SearchQuery{
		Fields: []string{
			"id", "dateAdded", "dateClosed", "dateEnd", "status",
			"categories", "employmentType", "title", "owner",
			"clientCorporation", "isOpen",
		},
		Where: "id>0",
		Start: 0,
		Count: p.bullhornRecordCount,
	}

	for {
		jobs, err := p.bullhornClient.JobOrderService.Search(ctx, query)
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

func (p Processor) buildDatasetSchema() *geckoboard.Dataset {
	return &geckoboard.Dataset{
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
}
