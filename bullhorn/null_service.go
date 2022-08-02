package bullhorn

import (
	"context"
	"errors"
)

var errMissingSession = errors.New("You must login with the auth service before using this service")

type nullJobOrderService struct{ JobOrderService }
type nullPlacementService struct{ PlacementService }
type nullJobSubmissionService struct{ JobSubmissionService }
type nullClientContactService struct{ ClientContactService }

func (nullJobOrderService) Search(context.Context, SearchQuery) (*JobOrders, error) {
	return nil, errMissingSession
}

func (nullPlacementService) Search(context.Context, SearchQuery) (*Placements, error) {
	return nil, errMissingSession
}

func (nullJobSubmissionService) Search(context.Context, SearchQuery) (*JobSubmissions, error) {
	return nil, errMissingSession
}

func (nullClientContactService) Search(context.Context, SearchQuery) (*ClientContacts, error) {
	return nil, errMissingSession
}
