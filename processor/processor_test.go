package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"context"
	"errors"
	"fmt"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

var defaultMockProcessor = []datasetProcessor{mockDatasetProcessor{}}

func TestProcessor_New(t *testing.T) {
	bc := &bullhorn.Client{}
	gc := &geckoboard.Client{}

	p := New(bc, gc)

	assert.Equal(t, p.geckoboardClient, gc)
	assert.Assert(t, cmp.Len(p.processors, 1))
}

func TestProcessor_ProcessAll(t *testing.T) {
	t.Run("successfully creates dataset and pushes data", func(t *testing.T) {
		gc := geckoboard.New("", "")
		dataSent := false

		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(got *geckoboard.Dataset) error {
				want := &geckoboard.Dataset{
					Name: "mock-model",
					Fields: map[string]geckoboard.Field{
						"id":     {Name: "ID", Type: geckoboard.StringType},
						"field2": {Name: "Field 2", Type: geckoboard.DatetimeType, Optional: true},
						"field3": {Name: "Field 3", Type: geckoboard.NumberType, Optional: true},
					},
					UniqueBy: []string{"id"},
				}
				assert.DeepEqual(t, got, want)
				return nil
			},
			appendDataFn: func(_ *geckoboard.Dataset, data geckoboard.Data) error {
				dataSent = true

				assert.Equal(t, len(data), 2)
				assert.DeepEqual(t, data, geckoboard.Data{
					{
						"id":     "4345",
						"field2": "2022-05-05",
						"field3": 44,
					},
					{
						"id":     "5555",
						"field2": "2022-06-05",
						"field3": 66,
					},
				})
				return nil
			},
		}

		proc, logs := defaultNewProcessor(gc, defaultMockProcessor)
		proc.ProcessAll(context.Background())

		assert.DeepEqual(t, logs.msgs, []string{
			"Pushing 2 mock model records to geckoboard\n",
		})
		assert.Assert(t, dataSent)
	})

	t.Run("runs each processor in the list", func(t *testing.T) {
		gc := geckoboard.New("", "")

		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(got *geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(_ *geckoboard.Dataset, data geckoboard.Data) error {
				return nil
			},
		}

		calls := []string{}
		proc, logs := defaultNewProcessor(gc, []datasetProcessor{
			mockDatasetProcessor{
				queryDataFn: func() (geckoboard.Data, error) {
					calls = append(calls, "query mock 1")
					return geckoboard.Data{}, nil
				},
				schemaFn: func() *geckoboard.Dataset {
					return nil
				},
			},
			mockDatasetProcessor{
				queryDataFn: func() (geckoboard.Data, error) {
					calls = append(calls, "query mock 2")
					return geckoboard.Data{}, nil
				},
				schemaFn: func() *geckoboard.Dataset {
					return nil
				},
			},
		})

		proc.ProcessAll(context.Background())
		assert.DeepEqual(t, calls, []string{
			"query mock 1",
			"query mock 2",
		})
		assert.DeepEqual(t, logs.msgs, []string{
			"Pushing 0 mock model records to geckoboard\n",
			"Pushing 0 mock model records to geckoboard\n",
		})
	})

	t.Run("processes successfully when no records", func(t *testing.T) {
		dataSent := false

		gc := geckoboard.New("", "")
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(got *geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(_ *geckoboard.Dataset, data geckoboard.Data) error {
				dataSent = true
				assert.Equal(t, len(data), 0)
				assert.DeepEqual(t, data, geckoboard.Data{})
				return nil
			},
		}

		proc, logs := defaultNewProcessor(gc, []datasetProcessor{
			mockDatasetProcessor{
				queryDataFn: func() (geckoboard.Data, error) {
					return geckoboard.Data{}, nil
				},
			},
		})

		proc.ProcessAll(context.Background())
		assert.DeepEqual(t, logs.msgs, []string{
			"Pushing 0 mock model records to geckoboard\n",
		})
		assert.Assert(t, dataSent)
	})

	t.Run("logs the error when data query fails", func(t *testing.T) {
		proc, logs := defaultNewProcessor(geckoboard.New("", ""), []datasetProcessor{
			mockDatasetProcessor{
				queryDataFn: func() (geckoboard.Data, error) {
					return geckoboard.Data{}, errors.New("query failed")
				},
			},
		})

		proc.ProcessAll(context.Background())
		assert.DeepEqual(t, logs.msgs, []string{
			"Fetching data for mock model failed with error: query failed\n",
		})
	})

	t.Run("logs the error when geckoboard find or create dataset fails", func(t *testing.T) {
		gc := geckoboard.New("", "")
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(*geckoboard.Dataset) error {
				return errors.New("failed to create dataset")
			},
		}

		proc, logs := defaultNewProcessor(gc, defaultMockProcessor)
		proc.ProcessAll(context.Background())

		assert.DeepEqual(t, logs.msgs, []string{
			"Creating mock model dataset failed with error: failed to create dataset\n",
		})
	})

	t.Run("logs the error when geckoboard find or create dataset fails", func(t *testing.T) {
		gc := geckoboard.New("", "")
		gc.DatasetService = mockDatasetService{
			findOrCreateFn: func(*geckoboard.Dataset) error {
				return nil
			},
			appendDataFn: func(*geckoboard.Dataset, geckoboard.Data) error {
				return errors.New("push data error")
			},
		}

		proc, logs := defaultNewProcessor(gc, defaultMockProcessor)
		proc.ProcessAll(context.Background())

		assert.DeepEqual(t, logs.msgs, []string{
			"Pushing 2 mock model records to geckoboard\n",
			"Pushing mock model data failed with error: push data error\n",
		})
	})
}

func defaultNewProcessor(gc *geckoboard.Client, processors []datasetProcessor) (Processor, *mockLogPrinter) {
	mockPrinter := &mockLogPrinter{
		msgs: []string{},
	}

	return Processor{
		processors:       processors,
		geckoboardClient: gc,
		printer:          mockPrinter,
	}, mockPrinter
}

// Mock log printer
type mockLogPrinter struct {
	msgs []string
}

func (m *mockLogPrinter) Printf(format string, v ...interface{}) {
	m.msgs = append(m.msgs, fmt.Sprintf(format, v...))
}

// Mock Geckoboard service

type mockDatasetService struct {
	findOrCreateFn func(*geckoboard.Dataset) error
	appendDataFn   func(*geckoboard.Dataset, geckoboard.Data) error
}

func (m mockDatasetService) FindOrCreate(_ context.Context, dataset *geckoboard.Dataset) error {
	return m.findOrCreateFn(dataset)
}

func (m mockDatasetService) AppendData(_ context.Context, dataset *geckoboard.Dataset, data geckoboard.Data) error {
	return m.appendDataFn(dataset, data)
}

// Mock processor

type mockDatasetProcessor struct {
	queryDataFn func() (geckoboard.Data, error)
	schemaFn    func() *geckoboard.Dataset
}

func (m mockDatasetProcessor) String() string {
	return "mock model"
}

func (m mockDatasetProcessor) QueryData(context.Context) (geckoboard.Data, error) {
	if m.queryDataFn != nil {
		return m.queryDataFn()
	}

	return geckoboard.Data{
		{
			"id":     "4345",
			"field2": "2022-05-05",
			"field3": 44,
		},
		{
			"id":     "5555",
			"field2": "2022-06-05",
			"field3": 66,
		},
	}, nil
}

func (m mockDatasetProcessor) Schema() *geckoboard.Dataset {
	if m.schemaFn != nil {
		return m.schemaFn()
	}

	return &geckoboard.Dataset{
		Name: "mock-model",
		Fields: map[string]geckoboard.Field{
			"id": {
				Name: "ID",
				Type: geckoboard.StringType,
			},
			"field2": {
				Name:     "Field 2",
				Type:     geckoboard.DatetimeType,
				Optional: true,
			},
			"field3": {
				Name:     "Field 3",
				Type:     geckoboard.NumberType,
				Optional: true,
			},
		},
		UniqueBy: []string{"id"},
	}
}
