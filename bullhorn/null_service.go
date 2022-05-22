package bullhorn

import (
	"context"
	"errors"
)

var errMissingSession = errors.New("You must login with the auth service before using this service")

type nullSessionService struct {
	JobOrderService
}

func (nullSessionService) Search(context.Context, SearchQuery) (*JobOrders, error) {
	return nil, errMissingSession
}
