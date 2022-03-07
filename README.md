
# Musical-Project

Performs analytics and visualisations of music listening activities. 


# Project Structure

This repo (`tabulate-music`) is but one-half. The other half (`tabulate-scrobble`) is located in another repo, [here](https://github.com/ktingyew/tabulate-scrobble).



# tabulate-music (this repo)

Python script housed in Docker container to allow me to extract .mp3 and .flac music tags and uploads to BigQuery, a serverless data warehouse in Google Cloud Platform (GCP).



# Prerequisites

0. A folder of songs with the appropriate tags
1. Need a GCP Service Account with credentials to access the following resource/s:
    - BigQuery
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

## View logs
```
docker logs <name-of-container>
```
- `--tail` to set number of lines of logs to show for each container