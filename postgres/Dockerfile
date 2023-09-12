FROM postgres:13-alpine


# import data
# RUN rm /docker-entrypoint-initdb.d/*.sql.gz


WORKDIR /bucket
COPY *.sql.gz /docker-entrypoint-initdb.d/