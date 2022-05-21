package cmd

import (
	"bufio"
	"bullhorn-to-dataset/config"
	"log"
	"os"

	"github.com/spf13/cobra"
)

func PushCommand() *cobra.Command {
	var credsFromEnv bool
	conf := &config.Config{}

	cmd := &cobra.Command{
		Use:       "push",
		Short:     "Query Bullhorn data and push data to Geckoboard",
		ValidArgs: []string{"creds-from-env"},
		Run: func(cmd *cobra.Command, args []string) {
			if credsFromEnv {
				conf.LoadFromEnvs()
				if err := conf.Validate(); err != nil {
					log.Fatal(err)
				}
			} else {
				askQuestion(conf, &conf.BullhornUsername, "Bullhorn username")
				askQuestion(conf, &conf.BullhornPassword, "Bullhorn password")
				askQuestion(conf, &conf.GeckoboardAPIKey, "Geckoboard apikey")
			}

			//TODO: Run with config instance
		},
	}

	cmd.Flags().BoolVar(&credsFromEnv, "creds-from-env", false, "Read credentials from envs instead of user input")
	cmd.Flags().StringVar(&conf.GeckoboardHost, "geckoboard-host", "api.geckoboard.com", "Geckoboard host to push data to")
	cmd.Flags().StringVar(&conf.BullhornHost, "bullhorn-host", "universal.bullhornstaffing.com", "Bullhorn universal API host")

	return cmd
}

func askQuestion(conf *config.Config, attrRef *string, question string) {
	val, err := conf.ReadValueFromInput(bufio.NewReader(os.Stdin), question)
	if err != nil {
		log.Fatal(err)
	}

	*attrRef = val
}
