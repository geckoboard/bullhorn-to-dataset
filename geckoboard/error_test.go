package geckoboard

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestError_Error(t *testing.T) {
	err := Error{
		StatusCode: 400,
		Detail: Detail{
			Message: "missing field type",
		},
	}

	assert.Equal(t, err.Error(), `There was an error sending the data to Geckoboard's API: "missing field type": with response code 400`)
}
