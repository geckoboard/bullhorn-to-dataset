package bullhorn

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestNullSessionService(t *testing.T) {
	t.Run("returns a missing session error when job order", func(t *testing.T) {
		n := nullJobOrderService{}
		_, err := n.Search(context.Background(), SearchQuery{})
		assert.Error(t, err, errMissingSession.Error())
	})

	t.Run("returns a missing session error when placement", func(t *testing.T) {
		n := nullPlacementService{}
		_, err := n.Search(context.Background(), SearchQuery{})
		assert.Error(t, err, errMissingSession.Error())
	})

	t.Run("returns a missing session error when placement", func(t *testing.T) {
		n := nullJobSubmissionService{}
		_, err := n.Search(context.Background(), SearchQuery{})
		assert.Error(t, err, errMissingSession.Error())
	})
}
