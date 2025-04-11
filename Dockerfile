FROM python:3.11-alpine

COPY ./requirements.txt /requirements.txt
RUN python -m pip install -r /requirements.txt

ENTRYPOINT ["tail", "-f", "/dev/null"]
