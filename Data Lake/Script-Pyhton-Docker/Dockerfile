FROM python:3.8

ADD main.py .

COPY account-client.json .

RUN pip install google-cloud-storage

CMD [ "python", "./main.py" ]