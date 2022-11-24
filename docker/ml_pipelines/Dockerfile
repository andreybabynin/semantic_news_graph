FROM python:3.9

WORKDIR /code

COPY requirements_ml.txt /code/
COPY setup.py /code/
COPY src /code/src

RUN pip install --upgrade pip

RUN pip install -r requirements_ml.txt



