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
	contactCustomFieldRules = map[string]int{
		"Date":  3,
		"Float": 3,
	}
)

type clientContactProcessor struct {
	client *bullhorn.Client

	maxDatasetRecords int
	recordsPerPage    int
	customFields      customFields
}

func (clientContactProcessor) String() string {
	return "contact"
}

func (c *clientContactProcessor) QueryData(ctx context.Context) (geckoboard.Data, error) {
	if err := c.customFields.fetchAndValidateCustomFields(c.String(), contactCustomFieldRules); err != nil {
		return nil, err
	}

	contacts, err := c.queryClientContacts(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Queried", len(contacts), "contacts")

	maxIndex := int(math.Min(float64(len(contacts)), float64(c.maxDatasetRecords)))
	latestContacts := contacts[0:maxIndex]

	data := geckoboard.Data{}
	for _, cc := range latestContacts {
		entry := geckoboard.DataRow{
			"id":              strconv.Itoa(cc.ID),
			"date_added":      valueOrNil(cc.DateAdded.String()),
			"updated_at":      valueOrNil(cc.DateLastModified.String()),
			"date_last_visit": valueOrNil(cc.DateListVisit.String()),
			"division":        valueOrNotSet(cc.Division),
			"owner":           cc.Owner.FullName(),
			"source":          valueOrNotSet(cc.Source),
			"status":          cc.Status,
			"type":            valueOrNotSet(cc.Type),
		}

		c.customFields.extractCustomFieldData(cc, entry)
		data = append(data, entry)
	}

	return data, nil
}

func (c *clientContactProcessor) Schema() *geckoboard.Dataset {
	datasetFields := map[string]geckoboard.Field{
		"id": {
			Name:     "ID",
			Type:     geckoboard.StringType,
			Optional: false,
		},
		"date_added": {
			Name: "Date added", Type: geckoboard.DatetimeType,
			Optional: true,
		},
		"updated_at": {
			Name: "Updated at", Type: geckoboard.DatetimeType,
			Optional: true,
		},
		"date_last_visit": {
			Name: "Date last visit", Type: geckoboard.DatetimeType,
			Optional: true,
		},
		"division": {
			Name:     "Division",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"name": {
			Name:     "Name",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"owner": {
			Name:     "Owner",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"source": {
			Name:     "Source",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"status": {
			Name:     "Status",
			Type:     geckoboard.StringType,
			Optional: true,
		},
		"type": {
			Name:     "Type",
			Type:     geckoboard.StringType,
			Optional: true,
		},
	}

	c.customFields.extractCustomFieldsForSchema(datasetFields)

	return &geckoboard.Dataset{
		Name:     "bullhorn-contacts",
		Fields:   datasetFields,
		UniqueBy: []string{"id"},
	}
}

func (c *clientContactProcessor) queryClientContacts(ctx context.Context) ([]bullhorn.ClientContact, error) {
	var contacts []bullhorn.ClientContact

	queryFields := []string{
		"id", "dateAdded", "dateLastModified", "dateLastVisit", "name",
		"division", "source", "status", "owner", "type",
	}

	for _, f := range c.customFields {
		queryFields = append(queryFields, f.sanitized)
	}

	query := bullhorn.SearchQuery{
		Fields: queryFields,
		Where:  "isDeleted=false",
		Start:  0,
		Count:  c.recordsPerPage,
	}

	for {
		cs, err := c.client.ClientContactService.Search(ctx, query)
		if err != nil {
			return nil, err
		}

		contacts = append(contacts, cs.Items...)

		if len(cs.Items) < query.Count || len(contacts) >= c.maxDatasetRecords {
			return contacts, nil
		}

		query.Start = query.Count + query.Start
	}
}
