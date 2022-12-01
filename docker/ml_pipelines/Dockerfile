FROM pytorch/pytorch:1.13.0-cuda11.6-cudnn8-runtime

WORKDIR /code

COPY requirements_ml.txt /code/
COPY setup.py /code/
COPY src /code/src

RUN pip install --upgrade pip

RUN pip install -r requirements_ml.txt
