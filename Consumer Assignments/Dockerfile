FROM python:3.9

COPY consumer.py consumer.py

RUN pip install boto3

CMD ["python","consumer.py","-rq=cs5260-requests","-wt=widgets"]