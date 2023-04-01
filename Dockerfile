FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENV TZ="Europe/Moscow"
RUN date

WORKDIR /code


COPY ./requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project
COPY . /