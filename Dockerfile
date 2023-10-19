# FROM python:3.10

# WORKDIR /code

# COPY ./requirements.txt /code/

# RUN apt update -y
# RUN pip install --upgrade pip
# RUN pip install -r requirements.txt



FROM python:3.10-slim

RUN apt-get update \
    # dependencies for building Python packages
    # && apt-get install -y build-essential \
    # tools
    && apt-get install -y curl \
    && apt-get install -y zip 
    # && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
    # && rm -rf /var/lib/apt/lists/*


WORKDIR /code
ADD requirements.txt requirements.txt
RUN apt update -y
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
# COPY app.py app.py
CMD ["python", "-u", "app.py"]