package geckoboard

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
)

type DatasetService interface {
	FindOrCreate(context.Context, *Dataset) error
	AppendData(context.Context, *Dataset, Data) error
}

type datasetService struct {
	client           *Client
	maxRecordsPerReq int
	jsonMarshalFn    func(interface{}) ([]byte, error)
}

type DatasetType string
type FieldType string

const (
	NumberType   FieldType = "number"
	DatetimeType FieldType = "datetime"
	StringType   FieldType = "string"
)

type Dataset struct {
	Name     string           `json:"id"`
	Fields   map[string]Field `json:"fields"`
	UniqueBy []string         `json:"unique_by,omitempty"`
}

type Field struct {
	Type     FieldType `json:"type"`
	Name     string    `json:"name"`
	Optional bool      `json:"optional"`
}

type DataRow map[string]interface{}
type Data []DataRow
type DataPayload struct {
	Data Data `json:"data"`
}

func (d *datasetService) buildDatasetPath(dataset *Dataset, isData bool) string {
	base := fmt.Sprintf("/datasets/%s", dataset.Name)

	if isData {
		return base + "/data"
	}

	return base
}

func (d *datasetService) FindOrCreate(ctx context.Context, dataset *Dataset) error {
	b, err := d.jsonMarshalFn(dataset)
	if err != nil {
		return err
	}

	path := d.buildDatasetPath(dataset, false)
	req, err := d.client.buildRequest(http.MethodPut, path, bytes.NewReader(b))
	if err != nil {
		return err
	}

	return d.client.doRequest(req.WithContext(ctx))
}

func (d *datasetService) AppendData(ctx context.Context, dataset *Dataset, data Data) error {
	grps := len(data) / d.maxRecordsPerReq
	var payload DataPayload

	for i := 0; i <= grps; i++ {
		batch := d.maxRecordsPerReq * i

		if i == grps {
			if batch+1 <= len(data) {
				payload := DataPayload{Data: data[batch:]}
				if err := d.sendData(ctx, dataset, payload); err != nil {
					return err
				}
			}
		} else {
			payload = DataPayload{Data: data[batch : d.maxRecordsPerReq*(i+1)]}
			if err := d.sendData(ctx, dataset, payload); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *datasetService) sendData(ctx context.Context, dataset *Dataset, payload DataPayload) error {
	b, err := d.jsonMarshalFn(payload)
	if err != nil {
		return err
	}

	path := d.buildDatasetPath(dataset, true)
	req, err := d.client.buildRequest(http.MethodPost, path, bytes.NewReader(b))
	if err != nil {
		return err
	}

	return d.client.doRequest(req.WithContext(ctx))
}
