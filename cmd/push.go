package cmd

import (
	"bufio"
	"bullhorn-to-dataset/bullhorn"
	"bullhorn-to-dataset/config"
	"bullhorn-to-dataset/geckoboard"
	"bullhorn-to-dataset/processor"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
)

func PushCommand() *cobra.Command {
	var credsFromEnv, singleRun bool
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

			for {
				ctx := context.Background()
				fmt.Printf("Authenticating with Bullhorn...")

				bc := bullhorn.New(conf.BullhornHost)
				if err := bc.AuthService.Login(ctx, conf.BullhornUsername, conf.BullhornPassword); err != nil {
					fmt.Printf("Failed\n")
					log.Fatal(err)
				}

				fmt.Printf("Success\nQuerying data from Bullhorn\n")

				gc := geckoboard.New(conf.GeckoboardHost, conf.GeckoboardAPIKey)
				job := processor.New(bc, gc)
				if err := job.Process(ctx); err != nil {
					log.Fatal(err)
				}

				if singleRun {
					fmt.Println("Finished")
					return
				} else {
					fmt.Printf("Sleeping for 15mins")
					time.Sleep(15 * time.Minute)
				}
			}
		},
	}

	cmd.Flags().BoolVar(&credsFromEnv, "creds-from-env", false, "Read credentials from envs instead of user input")
	cmd.Flags().BoolVar(&singleRun, "single-run", false, "Run querying data from Bullhorn just once and exit")
	cmd.Flags().StringVar(&conf.GeckoboardHost, "geckoboard-host", "https://api.geckoboard.com", "Geckoboard host to push data to")
	cmd.Flags().StringVar(&conf.BullhornHost, "bullhorn-host", "https://universal.bullhornstaffing.com", "Bullhorn universal API host")

	return cmd
}

func askQuestion(conf *config.Config, attrRef *string, question string) {
	val, err := conf.ReadValueFromInput(bufio.NewReader(os.Stdin), question)
	if err != nil {
		log.Fatal(err)
	}

	*attrRef = val
}
