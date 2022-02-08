
# Musical-Project

Performs analytics and visualisations of music listening activities. 


# Project Structure

This repo contains a docker-compose file for 2 Docker modules:

1. `tabulate-music`, located in this repo
2. `tabulate-scrobble`, located in another repo, [here](https://github.com/ktingyew/tabulate-scrobble)


# tabulate-music (this repo)

Python script housed in Docker container to allow me to extract .mp3 and .flac music tags and uploads to BigQuery, a serverless data warehouse in Google Cloud Platform (GCP).



# Prerequisites

1. Need a GCP Service Account with credentials
2. [last.fm](https://www.last.fm/) account with API key

# To Run

## Configure environmental variables

Modify `.env.sample`, and rename to `.env`, which will be incorporated into `docker-compose.yml` file when it is ran.

## Build docker-compose
```
docker-compose build
```

## Run `daily_script.bat` file (or use Windows Task Scheduler to run)
- Modify `daily_script.bat` file to point to `docker-compose.yml` file

## View the logs (of both modules simultaneously)
```
docker-compose -f %DOCKER_COMPOSE_FILEPATH% logs --tail="all"
```
- `--tail` to set number of lines of logs to show for each container