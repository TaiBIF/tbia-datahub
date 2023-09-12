# FROM python:3.10

# WORKDIR /code

# COPY ./requirements.txt /code/

# RUN apt update -y
# RUN pip install --upgrade pip
# RUN pip install -r requirements.txt



FROM python:3.10-slim
WORKDIR /code
ADD requirements.txt requirements.txt
RUN apt update -y
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
# COPY app.py app.py
CMD ["python", "-u", "app.py"]