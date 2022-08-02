package processor

import (
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/geckoboard"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var customFieldRegexp = regexp.MustCompile(`^(custom)(Date|Text|Float)(\d{1,2})$`)

type customFieldError struct {
	entity string
	field  string

	fieldInvalid bool
	notSupported bool
	underRange   bool
	maxRange     int
}

func (e customFieldError) Error() string {
	if e.fieldInvalid {
		return fmt.Sprintf("unknown %s field %q, only customDate0, customText0 and customFloat0 are valid", e.entity, e.field)
	}

	if e.underRange {
		return fmt.Sprintf("custom %s field %q, is out of range min field number is 1", e.entity, e.field)
	}

	if e.notSupported {
		return fmt.Sprintf("custom field %q, is not supported for %s", e.field, e.entity)
	}

	return fmt.Sprintf("custom %s field %q, is out of range max field number is %d", e.entity, e.field, e.maxRange)
}

type customField struct {
	sanitized    string
	datasetField string
	structField  string
	fieldType    string
	displayName  string
}

type customFields []customField

func (cfs *customFields) fetchAndValidateCustomFields(entity string, rules map[string]int) error {
	env := os.Getenv(fmt.Sprintf("%s_CUSTOMFIELDS", strings.ToUpper(entity)))
	if env == "" {
		return nil
	}

	rawFields := strings.Split(env, ",")
	for _, f := range rawFields {
		field := strings.TrimSpace(f)

		if !customFieldRegexp.MatchString(field) {
			return customFieldError{
				entity:       entity,
				field:        field,
				fieldInvalid: true,
			}
		}

		parts := customFieldRegexp.FindStringSubmatch(field)
		num, _ := strconv.Atoi(parts[3])
		err := customFieldError{
			entity: entity,
			field:  field,
		}
		if num <= 0 {
			err.underRange = true
			return err
		}

		maxRange := rules[parts[2]]
		if maxRange == 0 {
			err.notSupported = true
			return err
		}

		if num > maxRange {
			err.maxRange = maxRange
			return err
		}

		*cfs = append(*cfs, customField{
			sanitized:    field,
			datasetField: strings.ToLower(strings.Join(parts[1:], "_")),
			structField:  strings.Title(field),
			displayName:  strings.Join(parts[1:], " "),
			fieldType:    parts[2],
		})
	}

	return nil
}

func (cfs customFields) extractCustomFieldData(entity interface{}, row geckoboard.DataRow) {
	ref := reflect.Indirect(reflect.ValueOf(entity))

	for _, f := range cfs {
		val := ref.FieldByName(f.structField)

		switch f.fieldType {
		case "Text":
			row[f.datasetField] = val.String()
		case "Float":
			row[f.datasetField] = val.Float()
		case "Date":
			epoch, _ := val.Interface().(bullhorn.EpochMilli)
			row[f.datasetField] = valueOrNil(epoch.String())
		}
	}
}

func (cfs customFields) extractCustomFieldsForSchema(fields map[string]geckoboard.Field) {
	for _, f := range cfs {
		switch f.fieldType {
		case "Text":
			fields[f.datasetField] = geckoboard.Field{
				Name:     f.displayName,
				Type:     geckoboard.StringType,
				Optional: true,
			}
		case "Float":
			fields[f.datasetField] = geckoboard.Field{
				Name:     f.displayName,
				Type:     geckoboard.NumberType,
				Optional: true,
			}
		case "Date":
			fields[f.datasetField] = geckoboard.Field{
				Name:     f.displayName,
				Type:     geckoboard.DatetimeType,
				Optional: true,
			}
		}
	}
}
