@echo off

rem Music-Project

set DOCKER_COMPOSE_FILEPATH="C:/Users/kting/Documents/GitHub/tabulate-music/docker-compose.yml"

docker-compose -f %DOCKER_COMPOSE_FILEPATH% up

echo Complete!