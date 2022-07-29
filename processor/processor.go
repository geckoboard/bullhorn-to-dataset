package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"bullhorn-to-dataset/printer"
	"context"
	"fmt"
)

const (
	maxRecordsPerPage = 200
	maxDatasetRecords = 5000
)

type datasetProcessor interface {
	fmt.Stringer

	QueryData(context.Context) (geckoboard.Data, error)
	Schema() *geckoboard.Dataset
}

// Processor contains clients to push and pull data
type Processor struct {
	geckoboardClient *geckoboard.Client
	processors       []datasetProcessor
	printer          printer.Printer
}

func New(bc *bullhorn.Client, gc *geckoboard.Client) Processor {
	return Processor{
		geckoboardClient: gc,
		processors: []datasetProcessor{
			jobOrderProcessor{
				client:            bc,
				maxDatasetRecords: maxDatasetRecords,
				ordersPerPage:     maxRecordsPerPage,
			},
		},
		printer: printer.LogPrinter{},
	}
}

// Process handles multiple dataset processors calling process
// on each of them and creating the dataset for each of them and pushing data.
// Doesn't block other processors if one of them was to fail
func (p Processor) ProcessAll(ctx context.Context) {
	for _, dp := range p.processors {
		data, err := dp.QueryData(ctx)
		if err != nil {
			p.printer.Printf("Fetching data for %s failed with error: %s\n", dp, err)
			continue
		}

		dataset := dp.Schema()
		if err := p.geckoboardClient.DatasetService.FindOrCreate(ctx, dataset); err != nil {
			p.printer.Printf("Creating %s dataset failed with error: %s\n", dp, err)
			continue
		}

		fmt.Printf("Pushing %d %s records to geckoboard", len(data), dp)
		if err := p.geckoboardClient.DatasetService.AppendData(ctx, dataset, data); err != nil {
			p.printer.Printf("Pushing %s data failed with error: %s\n", dp, err)
			continue
		}
	}
}

func valueOrNotSet(v string) string {
	if v == "" {
		return "(not set)"
	}

	return v
}

func valueOrNil(v string) *string {
	if v == "" {
		return nil
	}

	return &v
}
