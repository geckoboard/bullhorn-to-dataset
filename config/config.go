package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var errMissingValue = "missing value from config item: %s"

// Config stores bullhorn credentials and Geckoboard api key
type Config struct {
	// Bullhorn user credentials
	BullhornUsername string
	BullhornPassword string
	BullhornHost     string

	// GeckoboardAPIKey to push
	GeckoboardAPIKey string
	GeckoboardHost   string
}

// FromEnv reads secret config values from environment variables
func (c *Config) LoadFromEnvs() {
	c.BullhornUsername = os.Getenv("BULLHORN_USER")
	c.BullhornPassword = os.Getenv("BULLHORN_PASS")
	c.GeckoboardAPIKey = os.Getenv("GECKOBOARD_APIKEY")
}

// ReadValueFromInput reads secrets from stdin instead of using
// command args to prevent secrets being available in command history
func (c *Config) ReadValueFromInput(reader *bufio.Reader, question string) (string, error) {
	fmt.Printf("Enter your %s: ", question)
	v, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	// Remove newline and carriage return for windows from value
	v = strings.TrimRight(v, "\n")
	v = strings.TrimRight(v, "\r")

	return v, nil
}

// Validate returns an error if any of the config values are missing
func (c *Config) Validate() error {
	if c.BullhornUsername == "" {
		return fmt.Errorf(errMissingValue, "bullhorn username")
	}

	if c.BullhornPassword == "" {
		return fmt.Errorf(errMissingValue, "bullhorn password")
	}

	if c.BullhornHost == "" {
		return fmt.Errorf(errMissingValue, "bullhorn host")
	}

	if c.GeckoboardAPIKey == "" {
		return fmt.Errorf(errMissingValue, "geckoboard apikey")
	}

	if c.GeckoboardHost == "" {
		return fmt.Errorf(errMissingValue, "geckoboard host")
	}

	return nil
}
