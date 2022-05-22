package bullhorn

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestClient_New(t *testing.T) {
	t.Run("returns new client", func(t *testing.T) {
		url := "http://example.com"
		c := New(url)

		assert.Assert(t, c.client != nil)
		assert.Equal(t, c.token, "")

		authServ := c.AuthService.(*authService)
		assert.Equal(t, authServ.client, c)
		assert.Equal(t, authServ.baseURL, url)
	})

	t.Run("returns job service as null service", func(t *testing.T) {
		c := New("http://example.com")

		_, ok := c.JobOrderService.(nullSessionService)
		assert.Assert(t, ok)
	})
}
