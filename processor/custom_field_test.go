package processor

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestCustomFieldError_Error(t *testing.T) {
	defaultCustomFieldError := func(override customFieldError) customFieldError {
		return customFieldError{
			entity:       "testmock",
			field:        "customField1",
			fieldInvalid: override.fieldInvalid,
			underRange:   override.underRange,
			notSupported: override.notSupported,
			maxRange:     override.maxRange,
		}
	}
	specs := []struct {
		in      customFieldError
		name    string
		wantErr string
	}{
		{
			name:    "invalid custom field name",
			in:      defaultCustomFieldError(customFieldError{fieldInvalid: true}),
			wantErr: `unknown testmock field "customField1", only customDate0, customText0 and customFloat0 are valid`,
		},
		{
			name:    "custom date field under range",
			in:      defaultCustomFieldError(customFieldError{underRange: true}),
			wantErr: `custom testmock field "customField1", is out of range min field number is 1`,
		},
		{
			name:    "custom date field not supported",
			in:      defaultCustomFieldError(customFieldError{notSupported: true}),
			wantErr: `custom field "customField1", is not supported for testmock`,
		},
		{
			name:    "custom date field over range",
			in:      defaultCustomFieldError(customFieldError{maxRange: 13}),
			wantErr: `custom testmock field "customField1", is out of range max field number is 13`,
		},
	}

	for _, spec := range specs {
		t.Run(spec.name, func(t *testing.T) {
			assert.Equal(t, spec.in.Error(), spec.wantErr)
		})
	}
}
