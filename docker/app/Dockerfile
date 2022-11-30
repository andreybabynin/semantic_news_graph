FROM python:3.9

WORKDIR /code

COPY setup.py /code/
COPY src /code/src

RUN pip install --upgrade pip

RUN pip install -r /code/src/app/requirements.txt
