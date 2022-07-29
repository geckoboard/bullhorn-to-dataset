package bullhorn

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestEpochMilli_Time(t *testing.T) {
	t.Run("returns parsed time value", func(t *testing.T) {
		unix := EpochMilli(1653215692000)
		assert.DeepEqual(t, unix.Time(), time.Date(2022, 5, 22, 10, 34, 52, 0, time.UTC))
	})
}

func TestEpochMilli_String(t *testing.T) {
	t.Run("returns an empty string", func(t *testing.T) {
		e := EpochMilli(0)
		assert.Equal(t, e.String(), "")
	})

	t.Run("returns time format in RFC3339", func(t *testing.T) {
		e := EpochMilli(1659111234000)
		assert.Equal(t, e.String(), "2022-07-29T16:13:54Z")

	})
}
