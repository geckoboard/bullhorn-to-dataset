# Bullhorn to geckoboard dataset

Push Bullhorn data into your Geckoboard dataset

## Quickstart

### 1. Download the app

* macOS [x64](https://github.com/geckoboard/jnormington/bullhorn-to-dataset/releases/download/v0.0.1/bullhorn-to-dataset-darwin-amd64) / [arm64](https://github.com/geckoboard/jnormington/bullhorn-to-dataset/releases/download/v0.0.1/bullhorn-to-dataset-darwin-arm64)
* Linux [x86](https://github.com/geckoboard/jnormington/bullhorn-to-dataset/releases/download/v0.0.1/bullhorn-to-dataset-linux-386) / [x64](https://github.com/geckoboard/jnormington/bullhorn-to-dataset/releases/download/v0.0.1/bullhorn-to-dataset-linux-amd64)
* Windows [x86](https://github.com/geckoboard/jnormington/bullhorn-to-dataset/releases/download/v0.0.1/bullhorn-to-dataset-386.exe) / [x64](https://github.com/geckoboard/jnormington/bullhorn-to-dataset/releases/download/v0.0.1/bullhorn-to-dataset-windows-amd64.exe)

#### Make it executable (macOS / Linux)

On macOS and Linux you'll need to open a terminal and run `chmod u+x path/to/file` (replacing `path/to/file` with the actual path to your downloaded app) in order to make the app executable.

### Run the script

Open up a terminal (on linux/max) and a command prompt (on windows), and run your script.

```
./bullhorn-to-dataset push
```

When you run this - will ask for the username, password and geckoboard apikey. This will need to be input on every run.

```
Enter your Bullhorn username:
Enter your Bullhorn password:
Enter your Geckoboard apikey:
```

Should you not want to input these details all the time - please refer to environment variables section

#### Output

When the script is running there will be basic output of its progress... Similar to below, should an error occur, it will
output the error and any error message it can provide.

```
Authenticating with Bullhorn...Success
Querying data from Bullhorn
Queried 2 job orders
Pushing 2 records to geckoboard
Finished
```

#### Environment variables

If you wish, you can provide environment variables instead of needing input - this is useful for running on a server or service.
The following environment variables are required to be set.

GECKOBOARD_APIKEY=key
BULLHORN_USER=username
BULLHORN_PASS=password

To use the environment variables you will need to need to pass the switch `--creds-from-env` after the push command.

### Geckoboard API

Hopefully this is obvious, but this is where your Geckoboard API key goes. You can find yours [here](https://app.geckoboard.com/account/details).

### Bullhorn credentials

These are your credentials you use to login to your instance of Bullhorn, unfortunately they don't provide an API key. So use the credentials to login
over the API to retrieve an access token.

### Refresh time

By default this app will periodically pull data from Bullhorn and push to Geckoboard every 15 minutes.

If you plan to use your own scheduler like cron or something, then you may pass the switch `--single-run`

### Dataset

This creates a single dataset in your account called **bullhorn-joborders**
