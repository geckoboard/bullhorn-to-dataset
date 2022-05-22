package bullhorn

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestNullSessionService(t *testing.T) {
	t.Run("returns a missing session error", func(t *testing.T) {
		n := nullSessionService{}
		_, err := n.Search(context.Background(), SearchQuery{})
		assert.Error(t, err, errMissingSession.Error())
	})
}
