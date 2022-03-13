
# Musical-Project

Performs analytics and visualisations of music listening activities. 


# Project Structure

This repo (`tabulate-music`) is but one-half. The other half (`scrobble-cloud-func`) is located in another repo, [here](https://github.com/ktingyew/scrobble-cloud-func).



# tabulate-music (this repo)

Python script housed in Docker container that extracts music tags of [.mp3](https://en.wikipedia.org/wiki/ID3) and [.flac](https://en.wikipedia.org/wiki/Vorbis_comment) files in my library into a table (snapshot). This table is then uploaded to [BigQuery](https://cloud.google.com/bigquery), a serverless data warehouse in Google Cloud Platform (GCP).

Additionally, this script records diffs from snapshot to snapshot, which effectively allows for tracking of tag changes in music file over a time period (including adding and deleting of new songs).


# Prerequisites

0. A folder of songs with the appropriate tags (see `src/schema.yaml`)
1. Need a GCP Service Account with credentials to access the following resource/s:
    - BigQuery
2. [last.fm](https://www.last.fm/) account with API key (for the other repo, `scrobble-cloud-func`)

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