package bullhorn

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestError_Error(t *testing.T) {
	err := Error{
		StatusCode:  400,
		RequestPath: "some/path",
		Message:     "missing where query",
	}

	assert.Equal(t, err.Error(), `Bullhorn error: missing where query got response code 400 for request path "some/path"`)
}
