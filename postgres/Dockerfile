FROM postgres:13-alpine


# import data
RUN rm /docker-entrypoint-initdb.d/*.sql.gz


COPY *.sql.gz /docker-entrypoint-initdb.d/