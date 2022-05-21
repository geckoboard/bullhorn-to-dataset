package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestConfig_LoadFromEnvs(t *testing.T) {
	defer func() {
		os.Unsetenv("BULLHORN_USER")
		os.Unsetenv("BULLHORN_PASS")
		os.Unsetenv("GECKOBOARD_APIKEY")
	}()

	os.Setenv("BULLHORN_USER", "tester")
	os.Setenv("BULLHORN_PASS", "pa55")
	os.Setenv("GECKOBOARD_APIKEY", "1234")

	got := &Config{}
	want := &Config{
		BullhornUsername: "tester",
		BullhornPassword: "pa55",
		GeckoboardAPIKey: "1234",
	}

	got.LoadFromEnvs()
	assert.DeepEqual(t, got, want)
}

func TestConfig_ReadValueFromInput(t *testing.T) {
	conf := &Config{}

	t.Run("returns the user input with newline trimmed", func(t *testing.T) {
		got, err := conf.ReadValueFromInput(bufio.NewReader(strings.NewReader("my value\n")), "question")
		assert.NilError(t, err)
		assert.Equal(t, got, "my value")
	})

	t.Run("returns the user input carriage return trimmed", func(t *testing.T) {
		got, err := conf.ReadValueFromInput(bufio.NewReader(strings.NewReader("my value\n\r")), "question")
		assert.NilError(t, err)
		assert.Equal(t, got, "my value")
	})

	t.Run("returns an error", func(t *testing.T) {
		_, err := conf.ReadValueFromInput(bufio.NewReader(strings.NewReader("eof")), "question")
		assert.Error(t, err, io.EOF.Error())
	})
}

func TestConfig_Validate(t *testing.T) {
	specs := []struct {
		in  *Config
		out string
	}{
		{
			in: &Config{
				BullhornUsername: "test",
				BullhornPassword: "pa55",
				BullhornHost:     "example.com",
				GeckoboardAPIKey: "apikey",
				GeckoboardHost:   "example.com",
			},
			out: "",
		},
		{
			in:  &Config{},
			out: "bullhorn username",
		},
		{
			in:  &Config{BullhornUsername: "test"},
			out: "bullhorn password",
		},
		{
			in: &Config{
				BullhornUsername: "test",
				BullhornPassword: "pa55",
			},
			out: "bullhorn host",
		},
		{
			in: &Config{
				BullhornUsername: "test",
				BullhornPassword: "pa55",
				BullhornHost:     "example.com",
			},
			out: "geckoboard apikey",
		},
		{
			in: &Config{
				BullhornUsername: "test",
				BullhornPassword: "pa55",
				BullhornHost:     "example.com",
				GeckoboardAPIKey: "apikey",
			},
			out: "geckoboard host",
		},
	}

	for _, spec := range specs {
		t.Run("", func(t *testing.T) {
			got := spec.in.Validate()

			if spec.out == "" {
				assert.NilError(t, got)
				return
			}

			assert.Error(t, got, fmt.Sprintf(errMissingValue, spec.out))
		})
	}
}
