FROM python:3.10.2-slim-buster

ENV TZ=Asia/Singapore

ENV PROJ_WORKDIR="/code"
WORKDIR $PROJ_WORKDIR

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["/usr/local/bin/python", "./src/tabulate_my_music.py"]
